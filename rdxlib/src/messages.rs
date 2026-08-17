
#[derive(Debug, PartialEq)]
pub enum Message<A, R>
{
	Action(A),
	Runtime(R),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Operation<M> {
	Run(M),
	Stop,
}