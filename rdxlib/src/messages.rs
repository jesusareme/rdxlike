
#[derive(Debug, PartialEq)]
pub enum Message<A, R>
{
	Action(A),
	Runtime(R),
}