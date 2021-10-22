def process_queue(queue, func, num_workers=None):
    if not num_workers:
        num_workers = 5

    def process_elements(queue):
        while True:
            try:
                item = queue.get(timeout=1)
                func(item)
                queue.task_done()
            except Empty:
                break

    threads = [Thread(target=process_elements, args=(queue,)) for _ in range(num_workers)]
    for t in threads:
        t.start()
    queue.join()
    for t in threads:
        t.join()
for i in x:
   queue.put(i)
process_queue(queue, test)