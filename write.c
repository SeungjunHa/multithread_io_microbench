#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <x86intrin.h>
#include <cpuid.h>
#include <sched.h>
#include <omp.h>
#include <time.h>
#include <utmpx.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <linux/kernel.h> 
#include <sys/sysinfo.h> 
#include <sys/stat.h> 
#include <sys/types.h> 

//#define DEBUG
#ifdef DEBUG
#define debug_printf(...) printf(__VA_ARGS__)
#else
#define debug_printf(...) do { } while(0)
#endif

typedef struct __node {
    void *key;
    struct __node *next;
}Node;

typedef struct __queue {
    Node *front;
    Node *rear;
    unsigned int max_size;
}Queue;

Queue *Q;

void init_queue()
{
	printf("Init lock free queue\n");
    Q = (Queue *)malloc(sizeof(Queue));
    if (!Q) {
        printf("No memory\n");
        exit(1);
    }
    Q->front = (Node *)malloc(sizeof(Node));
    Q->rear = Q->front;
    Q->front->key = 0;
    Q->front->next = NULL;
}
void enqueue(void *key)
{
    Node *temp;
    Node *new = (Node *)malloc(sizeof(Node));
	debug_printf("enqueue : %s\n", (char *)key);
    new->key = key;
    new->next = NULL;
    while (1) {
        temp = Q->rear;
        if(__sync_bool_compare_and_swap(&(temp->next), NULL, new))
            break;
        else
            __sync_bool_compare_and_swap(&(Q->rear), temp, temp->next);
    }
    __sync_bool_compare_and_swap(&(Q->rear), temp, new);
}
void *dequeue()
{
    Node *item;
    void *val;
    while(1) {
        item = Q->front;
        if(item->next == NULL)
            return NULL;

        if(__sync_bool_compare_and_swap(&(Q->front), item, item->next))
            break;
    }
    val = (void *)item->next->key;
    free(item);
    return val;
}

void cpu_bind(int cpuid)
{
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(cpuid, &set);
	if(sched_setaffinity(getpid(), sizeof(cpu_set_t), &set) < 0){
		printf("\nUnable to set affinity\n");
		exit(EXIT_FAILURE);
	}
}

#define FILE_DIR "/mnt/numa0/pure/fileset/"
#define GB (1024*1024*1024UL)
#define MB (1024*1024UL)
#define KB (1024UL)

#define MAX_FILE_SIZE (10*KB)
#define MAX_IO_SIZE (4*KB)

#define NUM_OF_FILES 10
#define NUM_OF_WORKER 1

int max_cpu;
int fd[NUM_OF_FILES];
char file_name[NUM_OF_FILES][64];
pthread_t worker[NUM_OF_WORKER];
char *write_buf;
char *read_buf;

void init_rw_buf()
{
	printf("Init buffer start\n");
	write_buf = (char *)calloc(MAX_FILE_SIZE, sizeof(char));
	read_buf = (char *)calloc(MAX_FILE_SIZE, sizeof(char));
	memset(write_buf, 0x1, MAX_FILE_SIZE);
	printf("Init buffer finish\n");
}
static void init_name()
{
	printf("Prepare %d Files\n", NUM_OF_FILES);
	for(int i=0;i<NUM_OF_FILES;i++) {
		sprintf(file_name[i], FILE_DIR"FILE%d", i);
		enqueue(file_name[i]);
	}	
}
static int open_file(const char *s, int flags, mode_t mode)
{
	int fd =  open(s, flags, mode);
	if(fd < 0)
		perror("open");
	debug_printf("open %s : %d\n", s, fd);
	return fd;
}
static void close_file(int fd)
{
	close(fd);
	debug_printf("Close : %d", fd);
}
static int create_file(const char *s)
{
	int fd = open_file(s, O_RDWR | O_TRUNC | O_CREAT, 0644);
	if(fd < 0)
		perror("create");
	debug_printf("create %s : %d\n", s, fd);
	return fd;
}
static int remove_file(const char *s)
{
	int ret = remove(s);
	if(ret == -1)
		perror("remove");
	debug_printf("remove %s\n", s);
	return ret;
}
static size_t pwrite_file(int fd, size_t count, off_t offset)
{
	size_t ret = 0;
	ret = pwrite(fd, write_buf, count, offset);
	return ret;
}
static size_t pread_file(int fd, size_t count, off_t offset)
{
	size_t ret = 0;
	off_t file_end = lseek(fd, 0, SEEK_END);
	count = count+offset > file_end ? file_end-offset : count;
	ret = pread(fd, read_buf, count, offset);
	return ret;
}
struct master_data {
	pthread_cond_t all_slave_wakeup;
	pthread_cond_t all_slave_sleep;
	pthread_mutex_t master_mutex;
	unsigned int num_of_slave;
	unsigned int sleeping_slave;
};

struct master_slave_data {
	struct master_data *master;
	pthread_t worker;
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	int index;
	unsigned int count;
	unsigned int finish;
	size_t written_byte;
	size_t read_byte;
	char name[64];
};

void per_thread_flow(const char *s, struct master_slave_data *shared)
{
	size_t count;
	off_t offset;	
	size_t ret = 0;
	int fd = open_file(s, O_RDWR, 0644);
	for(int i=0;i<100;i++) {
		count = abs(rand()) % MAX_IO_SIZE;
		offset = abs(rand()) % MAX_FILE_SIZE;
		printf("fd : %d count : %ld offset : %ld\n", fd, count, offset);
		ret += pwrite_file(fd, count, offset);
	}
	shared->written_byte = ret;
	close(fd);
}

void *slave_thr_func(void *data)
{
	struct master_slave_data *shared = (struct master_slave_data *)data;
	struct master_data *master = (struct master_data *)shared->master;

	pthread_mutex_lock(&shared->mutex);	
	if(shared->count == 0) 
		pthread_cond_signal(&shared->cond);
	shared->count += 1;
	pthread_mutex_unlock(&shared->mutex);
			
	pthread_mutex_lock(&shared->mutex);	
	while(shared->count == 0)
		pthread_cond_wait(&shared->cond, &shared->mutex);
	shared->count -= 1;
	printf("Slave Wake up then open (%s) %d\n", shared->name, sched_getcpu());
	pthread_mutex_unlock(&shared->mutex);
	
	pthread_mutex_lock(&master->master_mutex);
	master->sleeping_slave += 1;
	printf("Slave sleep..(%d)\n", master->sleeping_slave);
	if(master->sleeping_slave >= master->num_of_slave)
		pthread_cond_signal(&master->all_slave_sleep);	
	pthread_cond_wait(&master->all_slave_wakeup, &master->master_mutex);
	pthread_mutex_unlock(&master->master_mutex);

	per_thread_flow(shared->name, shared);

	pthread_mutex_lock(&shared->mutex);	
	if(shared->finish == 0)
		pthread_cond_signal(&shared->cond);
	shared->finish = 1;
	pthread_mutex_unlock(&shared->mutex);
}

void *master_thr_func(void *data)
{
	char *thr_name = (char *)data;
	printf("%s is init running on (%d)\n", thr_name, sched_getcpu());

	int num_of_slave = 2;
	struct master_data *private;
	struct master_slave_data *shared;
	private = (struct master_data *)malloc(sizeof(struct master_data));
	shared = (struct master_slave_data *)malloc(
							sizeof(struct master_slave_data)*num_of_slave);
	pthread_cond_init(&private->all_slave_wakeup, NULL);
	pthread_cond_init(&private->all_slave_sleep, NULL);
	pthread_mutex_init(&private->master_mutex, NULL);
	private->num_of_slave = num_of_slave;
	private->sleeping_slave = 0;
	for(int i=0;i<num_of_slave;i++) {
		shared[i].master = private;
		pthread_cond_init(&shared[i].cond, NULL);
		pthread_mutex_init(&shared[i].mutex, NULL);
		shared[i].index = i;
		shared[i].count = 0;
		shared[i].finish = 0;
		shared[i].written_byte = 0;
		shared[i].read_byte = 0;
	}
	int random_cpu[num_of_slave];
	cpu_set_t cpu_set[num_of_slave];
	max_cpu = get_nprocs(); // get_nprocs_conf();
	for(int i=0;i<num_of_slave;i++) {
		random_cpu[i] = rand() % max_cpu;
		CPU_ZERO(&cpu_set[i]);
		CPU_SET(random_cpu[i], &cpu_set[i]);
	}
	int ret;
	for(int i=0;i<num_of_slave;i++) {
		pthread_mutex_lock(&shared[i].mutex);
		ret = pthread_create(&shared[i].worker, NULL, slave_thr_func, &shared[i]);
		while(shared[i].count == 0)
			pthread_cond_wait(&shared[i].cond, &shared[i].mutex);
		shared[i].count -= 1;
		pthread_mutex_unlock(&shared[i].mutex);
		if (ret < 0) {
			perror("pthread_create");
			break;
		}
		ret = pthread_setaffinity_np(shared[i].worker, sizeof(cpu_set[i]), &cpu_set[i]);
		if (ret != 0) {
			perror("pthread_setaffinity_np");
			break;
		}
	}

	char *name = (char *)dequeue();
	create_file(name);
	if(name != NULL) {
		size_t total_ret = 0;
		for(int i=0;i<num_of_slave;i++) {
			memcpy(shared[i].name, name, strlen(name));
			shared[i].name[strlen(name)] = '\0';
			printf("open : (%s) (%s)\n", name, shared[i].name);
			pthread_mutex_lock(&shared[i].mutex);
			if(shared[i].count == 0) 
				pthread_cond_signal(&shared[i].cond);	
			shared[i].count += 1;
			pthread_mutex_unlock(&shared[i].mutex);
		}
		pthread_mutex_lock(&private->master_mutex);
		while(private->sleeping_slave < num_of_slave)
			pthread_cond_wait(&private->all_slave_sleep, &private->master_mutex);
		pthread_cond_broadcast(&private->all_slave_wakeup);
		printf("Whole worker is wake up\n");
		pthread_mutex_unlock(&private->master_mutex);
		for(int i=0;i<num_of_slave;i++) {
			pthread_mutex_lock(&shared[i].mutex);
			while(shared[i].finish == 0) 
				pthread_cond_wait(&shared[i].cond, &shared[i].mutex);	
			shared[i].finish -= 1;
			pthread_mutex_unlock(&shared[i].mutex);
			printf("%d's ret : %ld\n", i, shared[i].written_byte);
			total_ret += shared[i].written_byte;
		}
		printf("Remove file %ld\n", total_ret);
		for(int i=0;i<num_of_slave;i++) 
			pthread_join(shared[i].worker, NULL);
		printf("All Flow is finished %s\n", thr_name);
		//remove_file(name);
		//enqueue(name);
	} 

	pthread_exit(NULL);
}
static int create_worker()
{
	int ret = 0;
	int random_cpu[NUM_OF_WORKER];
	cpu_set_t cpu_set[NUM_OF_WORKER];
	max_cpu = get_nprocs(); // get_nprocs_conf();

	for(int i=0;i<NUM_OF_WORKER;i++) {
		random_cpu[i] = rand() % max_cpu;
		CPU_ZERO(&cpu_set[i]);
		CPU_SET(random_cpu[i], &cpu_set[i]);
	}

	for(int i=0;i<NUM_OF_WORKER;i++) {
		char *n = (char *)calloc(15, sizeof(char));
		sprintf(n, "worker%d", i);
		ret = pthread_create(&worker[i], NULL, master_thr_func, (void *)n);
		if (ret < 0) {
			perror("pthread_create");
			break;
		}
		ret = pthread_setaffinity_np(worker[i], sizeof(cpu_set[i]), &cpu_set[i]);
		if (ret != 0)
			perror("pthread_setaffinity_np");
	}

	char *s =  ret < 0 ? "Fail to Create Worker\n" : "Success to Create Worker\n";
	printf("%s", s);
	return ret;
}
void join_worker()
{
	int i;
	printf("Waiting worker...\n");
	for(i=0;i<NUM_OF_WORKER;i++) 
		pthread_join(worker[i], NULL);
	printf("Finish whole worker!\n");
}
int init_directory()
{
	int ret;
	char system_buf[100];
	ret = access(FILE_DIR, F_OK);
	if(ret == 0) {
		sprintf(system_buf, "rm -rf %s", FILE_DIR);
		ret = system(system_buf);
		printf("Remove Already Exist Directory(%d) %s\n", ret, FILE_DIR);
	} 
	ret = mkdir(FILE_DIR, 0755);
	printf("Create Directory(%d) %s\n", ret, FILE_DIR);	
	return ret;
}
int main()
{
	printf("Start Program\n");
	srand(time(NULL));
	int ret, i, j, random, cnt = 0;

	ret = init_directory();
	if(ret == -1)
		goto out;

    init_queue();
	init_rw_buf();
	init_name();

	ret = create_worker();
	if(ret < 0)
		goto out;

	join_worker();
out:
	printf("Exit Program\n");
	return 0;
}
	/*
	int lock = 0;
	int asdf = 0;
	if(atomic_compare_exchange_strong(&lock, &asdf, 1))
		printf("change : %d\n", lock);
	else
		printf("no : %d\n", lock);

	if(atomic_compare_exchange_strong(&lock, &asdf, 1))
		printf("change : %d\n", lock);
	else
		printf("no : %d\n", lock);

	err = open(FILE_NAME, O_RDWR | O_TRUNC | O_CREAT, 0644);
	if(err < 0) {
		printf("Fail to Create %s\n", FILE_NAME);
		goto out;
	} else {
		printf("Success to Create %s\n", FILE_NAME);	
		int tt = 0;
	//int arr[10] = {16,16,16,16,16,16,16,16,16,16};
		while(max > 0) {
			random = abs(rand() % 16*1024 + 1);
			int randomcpu = abs(rand() % 32);
			//randomcpu = arr[tt++];
			cpu_bind(randomcpu);
			//random = 4096;
			ret = write(err, write_buf+ret, random);
			printf("[%d] Attempt : %d / Return : %d\n", randomcpu, random, ret);
			cnt+=ret;	
			max -= ret;
		}
	}
	printf("Total Written : %d\n", cnt);
	int read_byte = 0;
	err = open(FILE_NAME, O_RDONLY);
	if(err < 0) {
		printf("Open Failed %d\n", err);
	} else {
		int tt = 0;
		//int arr[10] = {0,0,0,0,0,16,16,16,16,16};
		//while(cnt) {
		for(int i=0;i<10;i++) {
			for(int j=0;j<2;j++) {
				random = abs(rand() % 20000);
				int randomcpu = abs(rand() % 32);
				//randomcpu = arr[tt++];
				cpu_bind(randomcpu);
				if(j == 0)
					random = 1000;
				else
					random = 4096*100;
				ret = read(err, read_buf, random);
				read_byte += ret;
				printf("[%d]Read Attempt : %d , Return Bytes : %d\n", 
						randomcpu, random, ret);
				cnt -= ret;
				if(ret <= 0)
					break;
			}
			lseek(err, 0, SEEK_SET);
		}
		//}
		printf("Total Read : %d\n", read_byte);
		close(err);
	}*/
	/*ret = unlink(FILE_NAME);
	if(ret != -1)
		printf("Success to Unlink %s\n", FILE_NAME);
	else	
		printf("Fail to Unlink %s\n", FILE_NAME);*/
