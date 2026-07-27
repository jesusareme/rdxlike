pub enum Cmd<A, SC>
where
    SC: SCmd,
{
    Direct(Vec<A>),
    Queue(Vec<A>),
    Async(Box<dyn FnOnce() -> A + Send + 'static>),
    Env(SC),
}

impl<A, SC: SCmd> From<SC> for Cmd<A, SC> {
    fn from(value: SC) -> Self {
        Cmd::Env(value)
    }
}

pub trait SCmd {
    type Environment;

    fn process(self, env: &mut Self::Environment);
}