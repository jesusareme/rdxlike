use crate::MoniError;
use crate::runtime::{LongLivingTasks, ModelState, RunningState};
use jiff::Zoned;

pub struct ClockedModelStateView<'a> {
    pub(crate) model_state: &'a mut ModelState,
    pub(crate) time: &'a Zoned,
    pub(crate) errors: &'a mut Vec<MoniError>,
    pub(crate) tasks: &'a mut LongLivingTasks, 
}

impl<'a> ClockedModelStateView<'a> {
    pub fn new(model: &'a mut ModelState, running_state: &'a mut RunningState) -> Self {
        ClockedModelStateView {
            model_state: model,
            time: &running_state.time,
            errors: &mut running_state.errors,
            tasks: &mut running_state.tasks,
        }
    }
}
