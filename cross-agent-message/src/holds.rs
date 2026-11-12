//! Pure rules for holds, applied inside a store transaction.

use protocol::{Hold, Release, Take, Taken};
use std::collections::HashMap;

/// What a [`Take`] of `holds` at `now` comes to: the hold is `take.holder`'s
/// where it was free, lapsed or its own already, until `take.minutes` from
/// now, held since it first took it; another agent's is left as it is.
pub fn take_hold(holds: &mut HashMap<String, Hold>, take: &Take, now: u64) -> Taken {
    let held = holds.get(&take.name).filter(|hold| hold.until > now);
    if let Some(hold) = held.filter(|hold| hold.holder != take.holder) {
        return Taken {
            hold: hold.clone(),
            yours: false,
        };
    }
    let hold = Hold {
        name: take.name.clone(),
        holder: take.holder.clone(),
        since: held.map_or(now, |hold| hold.since),
        until: now + u64::from(take.minutes) * 60_000,
    };
    holds.insert(take.name.clone(), hold.clone());
    Taken { hold, yours: true }
}

/// `release.holder` lets go of its hold at `now`: gone, as one that lapsed
/// or was never taken is already. Another agent's is refused, naming it.
pub fn release_hold(
    holds: &mut HashMap<String, Hold>,
    release: &Release,
    now: u64,
) -> Result<(), String> {
    match holds.get(&release.name).filter(|hold| hold.until > now) {
        Some(hold) if hold.holder != release.holder => Err(format!(
            "{} holds {}, not {}: only its holder lets it go",
            hold.holder, release.name, release.holder
        )),
        _ => {
            holds.remove(&release.name);
            Ok(())
        }
    }
}

/// What's held at `now`, by name: a hold that lapsed is forgotten.
pub fn held(holds: &mut HashMap<String, Hold>, now: u64) -> Vec<Hold> {
    holds.retain(|_, hold| hold.until > now);
    let mut held: Vec<Hold> = holds.values().cloned().collect();
    held.sort_by(|a, b| a.name.cmp(&b.name));
    held
}
