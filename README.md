# multithread_io_microbench
filebench webproxy을 흉내냄

File name is created at the first time of program.
The set of file name are enqueued in lock-free queue using CAS.
Based on Implementing Lock-Free Queues (by John D. Valois).

The types of thread structure is detach-worker.
N detach thread is created from main, and they create their own worker thread.

Detach thread dequeue the name of target file and send it to its own worker thread.
Every worker thread perform multithread io simultaneously...

Still work...
