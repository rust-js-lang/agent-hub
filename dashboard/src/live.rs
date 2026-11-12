//! Own the dashboard stream, its one recovery timer, and probe generations.
use std::{cell::RefCell, rc::Rc};

use js::{TimeoutId, clear_timeout, set_timeout, spawn};
use protocol::endpoints;
use webapi::{Event, EventSource, EventTargetExt, events};

use crate::{api, client};

struct State {
    source: Option<&'static EventSource>,
    timer: Option<&'static TimeoutId>,
    generation: u32,
    backoff: u8,
    stopped: bool,
    hub: Rc<dyn Fn(protocol::HubInfo)>,
    live: Rc<dyn Fn(bool)>,
    refresh: Rc<dyn Fn()>,
    problem: Rc<dyn Fn(String)>,
}

pub struct Subscription {
    state: Rc<RefCell<State>>,
}

impl Subscription {
    pub fn stop(&self) {
        stop(&self.state);
    }
}

pub fn listen(
    hub: impl Fn(protocol::HubInfo) + 'static,
    live: impl Fn(bool) + 'static,
    refresh: impl Fn() + 'static,
    problem: impl Fn(String) + 'static,
) -> Subscription {
    let state = Rc::new(RefCell::new(State {
        source: None,
        timer: None,
        generation: 0,
        backoff: 0,
        stopped: false,
        hub: Rc::new(hub),
        live: Rc::new(live),
        refresh: Rc::new(refresh),
        problem: Rc::new(problem),
    }));
    probe(&state, Probe::Mount);
    Subscription { state }
}

fn current(state: &Rc<RefCell<State>>, source: &EventSource) -> bool {
    let state = state.borrow();
    !state.stopped && state.source.is_some_and(|active| active == source)
}

fn stop(state: &Rc<RefCell<State>>) {
    let (source, timer) = {
        let mut state = state.borrow_mut();
        state.stopped = true;
        state.generation = state.generation.wrapping_add(1);
        (state.source.take(), state.timer.take())
    };
    clear_timeout(timer);
    if let Some(source) = source {
        source.close();
    }
}

fn connect(state: &Rc<RefCell<State>>, advertised: client::Advertised) {
    let (previous, timer) = {
        let mut state = state.borrow_mut();
        if state.stopped {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        (state.source.take(), state.timer.take())
    };
    clear_timeout(timer);
    if let Some(previous) = previous {
        previous.close();
    }
    let frames = Rc::clone(state);
    let source = client::stream(
        &endpoints::EVENTS,
        &endpoints::Events {
            agent: None,
            protocol: Some(protocol::PROTOCOL),
            types: Some(
                protocol::EVENT_TYPES
                    .split(',')
                    .map(str::to_string)
                    .collect(),
            ),
        },
        &advertised,
        move |source, event| {
            if !current(&frames, source) {
                return;
            }
            match event {
                Ok(_) => {
                    let refresh = Rc::clone(&frames.borrow().refresh);
                    refresh();
                }
                Err(reason) => {
                    let problem = Rc::clone(&frames.borrow().problem);
                    problem(reason);
                }
            }
        },
    );
    let source = match source {
        Ok(source) => source,
        Err(error) => {
            let problem = Rc::clone(&state.borrow().problem);
            problem(error.message());
            schedule(state, Next::Probe(Probe::Mount));
            return;
        }
    };
    state.borrow_mut().source = Some(source);
    let opened = Rc::clone(state);
    source.add_event_listener(
        events::Open,
        move |_: &Event| {
            if !current(&opened, source) {
                return;
            }
            let (live, refresh) = {
                let mut state = opened.borrow_mut();
                state.backoff = 0;
                (Rc::clone(&state.live), Rc::clone(&state.refresh))
            };
            live(true);
            refresh();
            probe(&opened, Probe::Open);
        },
        None,
    );
    let failed = Rc::clone(state);
    source.add_event_listener(
        events::Error,
        move |_: &Event| {
            if !current(&failed, source) {
                return;
            }
            let live = Rc::clone(&failed.borrow().live);
            live(false);
            // CONNECTING belongs to the browser's own retry loop. Only CLOSED
            // needs our probe/recreate loop, since the browser has stopped.
            if source.ready_state() == 2.0 {
                probe(&failed, Probe::Closed);
            }
        },
        None,
    );
}

#[derive(Clone, Copy)]
enum Probe {
    Mount,
    Open,
    Closed,
}

enum Next {
    Connect(client::Advertised),
    Probe(Probe),
}

fn schedule(state: &Rc<RefCell<State>>, next: Next) {
    let (previous, generation, delay) = {
        let mut state = state.borrow_mut();
        if state.stopped {
            return;
        }
        let delay = match state.backoff {
            0 => 1,
            1 => 2,
            2 => 5,
            3 => 10,
            4 => 20,
            _ => 30,
        };
        state.backoff = (state.backoff + 1).min(5);
        state.generation = state.generation.wrapping_add(1);
        (state.timer.take(), state.generation, delay)
    };
    clear_timeout(previous);
    let waiting = Rc::clone(state);
    let timer = set_timeout(
        move || {
            {
                let mut state = waiting.borrow_mut();
                if state.stopped || state.generation != generation {
                    return;
                }
                state.timer = None;
            }
            match next {
                Next::Connect(advertised) => connect(&waiting, advertised),
                Next::Probe(kind) => probe(&waiting, kind),
            }
        },
        Some(f64::from(delay) * 1000.0),
    );
    state.borrow_mut().timer = Some(timer);
}

fn probe(state: &Rc<RefCell<State>>, kind: Probe) {
    let (previous, generation) = {
        let mut state = state.borrow_mut();
        if state.stopped {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        (state.timer.take(), state.generation)
    };
    clear_timeout(previous);
    let pending = Rc::clone(state);
    spawn(Box::new(async move {
        let answer = client::probe().await;
        {
            let state = pending.borrow();
            if state.stopped || state.generation != generation {
                return;
            }
        }
        match answer {
            Err(error) if error.upgrade_required() => {
                stop(&pending);
                api::reload();
            }
            Ok(advertised) => {
                let hub = Rc::clone(&pending.borrow().hub);
                hub(advertised.info().clone());
                match kind {
                    Probe::Mount => connect(&pending, advertised),
                    Probe::Closed => schedule(&pending, Next::Connect(advertised)),
                    Probe::Open => {}
                }
            }
            Err(_) => match kind {
                Probe::Mount | Probe::Closed => schedule(&pending, Next::Probe(kind)),
                Probe::Open => {} // An open stream probes at its next open/error.
            },
        }
    }));
}
