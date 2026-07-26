use crate::types::mining_task::MiningTask;

// Simple FIFO-ish queue of pending mining tasks, keyed by voter_id so a voter
// can never have more than one task queued at once.
pub struct MiningPool {
    tasks: Vec<MiningTask>,
}

impl MiningPool {
    pub fn new() -> Self {
        Self { tasks: vec![] }
    }

    pub fn contains_voter(&self, voter_id: &str) -> bool {
        self.tasks.iter().any(|t| t.voter_id == voter_id)
    }

    // Adds the task unless one from the same voter is already queued.
    // Returns whether the task was added.
    pub fn add_task(&mut self, task: MiningTask) -> bool {
        if self.contains_voter(&task.voter_id) {
            return false;
        }
        self.tasks.push(task);
        true
    }

    // Removes and returns the most recently added task.
    pub fn take_last(&mut self) -> Option<MiningTask> {
        self.tasks.pop()
    }

    // Puts a task back in the pool, e.g. after a failed mining attempt.
    pub fn requeue(&mut self, task: MiningTask) {
        self.tasks.push(task);
    }
}
