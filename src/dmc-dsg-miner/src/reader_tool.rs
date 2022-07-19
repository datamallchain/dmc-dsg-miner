use async_std::io::Read;
use std::{collections::LinkedList,task::{Poll, Context},pin::Pin};

pub struct ReaderTool;
impl ReaderTool {
    pub async fn merge(data: Vec<Box<dyn Unpin + Read + Send + Sync>>) -> impl Read + Send + Sync {
        MergeReader::new(data)
    }
}

pub struct MergeReader {
    list: LinkedList<Box<dyn Unpin + Read + Send + Sync>>
}

impl MergeReader {
    pub fn new(list: Vec<Box<dyn Unpin + Read + Send + Sync>>) -> Self {
        let mut link = LinkedList::new();
        link.extend(list);
        Self { list: link }
    }
}

impl Read for MergeReader {
    fn poll_read(mut self: Pin<&mut Self>, ctx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, std::io::Error>> {
        while let Some(mut reader) = self.list.pop_front() {
            match Pin::new(&mut reader).poll_read(ctx, buf) {
                Poll::Ready(r) => {
                    match r {
                        Ok(len) => {
                            if len > 0 {
                                self.list.push_front(reader);
                                return Poll::Ready(Ok(len))
                            } else {
                                continue;
                            }
                        },
                        Err(e) => {
                            self.list.push_front(reader);
                            return Poll::Ready(Err(e))
                        }
                    }
                },
                Poll::Pending => {
                    self.list.push_front(reader);
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(0))
    }
}
