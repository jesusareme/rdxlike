
#[derive(Debug)]
pub enum Message<A, R>
{
	Action(A),
	Runtime(R),
}