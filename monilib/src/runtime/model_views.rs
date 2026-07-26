use crate::runtime::ModelState;
use jiff::Zoned;
use crate::MoniError;

pub struct ClockedModelStateView<'a> {
    pub(crate) model_state: &'a mut ModelState,
    pub(crate) time: &'a Zoned,
    pub(crate) errors: &'a mut Vec<MoniError>
}
