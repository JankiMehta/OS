#include "disk_scheduler.h"
#include "disk.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#define REQUEST_READ 0
#define REQUEST_WRITE 1

/*
A request object contains:
read_write = 0 for read, 1 for write
block1 = the block to be read/written
wdata = const data to be written
rdata = non-const data to be read
*/
struct request {
	int read_write;
	int block1;
	const char *wdata;
	char *rdata;
};

// linked list of requests
struct linked {
	struct request *val;
	struct linked *next;
	struct linked *prev;
};
typedef struct linked req;

// for use with scan and sstf
int nextblock = 0, nextdir = 1;

/*
A disk_scheduler objects contains a pointer to a disk,
a record of the scheduling mode, and pointers to the
head and tail of the linked list of request objects.
*/

struct disk_scheduler {
	struct disk *disk;
	disk_scheduler_mode_t mode;
	req *head, *tail;
};

// monitor variables
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  cond  = PTHREAD_COND_INITIALIZER;

/*
Create a new disk scheduler object and initialize its fields.
*/
struct disk_scheduler * disk_scheduler_create( const char *filename, int nblocks, disk_scheduler_mode_t mode )
{
	struct disk_scheduler *s = malloc(sizeof(*s));
	if(!s) return 0;

	s->disk = disk_create(filename,nblocks);
	if(!s->disk) {
		free(s);
		return 0;
	}

	s->mode = mode;
	s->head = NULL;
	s->tail = NULL;

	return s;
}

/*
Return the number of blocks in the disk.
*/
int disk_scheduler_nblocks( struct disk_scheduler *s )
{
	return disk_nblocks(s->disk);
}

/*
Add a write request object to a data structure, wait for 
the scheduler thread to service it, and then return back 
to the calling program.
*/
void disk_scheduler_write( struct disk_scheduler *s, int block, const char *data )
{	
	// lock the mutex
	if( pthread_mutex_lock( &mutex )!=0 ) {
		fprintf(stderr,"Mutex lock failed: %s\n",strerror(errno));
		exit(1);
	}
	
	// critical code
	// allocate the request object
	struct request *req1;
	if(( req1 = (struct request *)malloc(sizeof(struct request)))==NULL ) {
		fprintf(stderr,"Struct malloc error: %s",strerror(errno));
		exit(1);
	}
	req1->block1 = block;
	req1->wdata = data;
	req1->read_write = REQUEST_WRITE;
	
	// allocate the linked list object
	req *curr;
	if(( curr = (req *)malloc(sizeof(req)))==NULL ) {
		fprintf(stderr,"Request malloc error: %s",strerror(errno));
		exit(1);
	}
		
	// add a request object to a data structure
	curr->val = req1;
	curr->next = NULL;
	curr->prev = NULL;
	if( !s->head ) {
		s->head = curr;
		s->tail = curr;
	}
	else {
		s->tail->next = curr;
		curr->prev = s->tail;
		s->tail = curr;
	}
	//printf("write block %d starting\n",block);
	// wait for the scheduler thread to service it
	if( pthread_cond_wait( &cond, &mutex )!= 0 ) {
		fprintf(stderr,"Cond wait failed: %s\n",strerror(errno));
		exit(1);
	}
	
	// unlock the mutex
	if( pthread_mutex_unlock( &mutex )!=0 ) {
		fprintf(stderr,"Mutex unlock failed: %s\n",strerror(errno));
		exit(1);
	}
	//printf("write block %d done\n",block);
}

/*
Add a read request object to a data structure, wait for 
the scheduler thread to service it, and then return back 
to the calling program.
*/
void disk_scheduler_read( struct disk_scheduler *s, int block, char *data )
{
	// lock the mutex
	if( pthread_mutex_lock( &mutex )!=0 ) {
		fprintf(stderr,"Mutex lock failed: %s\n",strerror(errno));
		exit(1);
	}
	
	// critical code
	// allocate the request object
	struct request *req1;
	if(( req1 = (struct request *)malloc(sizeof(struct request)))==NULL ) {
		fprintf(stderr,"Struct malloc error: %s",strerror(errno));
		exit(1);
	}
	req1->block1 = block;
	req1->rdata = data;
	req1->read_write = REQUEST_READ;
	
	// allocate the linked list object
	req *curr;
	if(( curr = (req *)malloc(sizeof(req)))==NULL ) {
		fprintf(stderr,"Request malloc error: %s",strerror(errno));
		exit(1);
	}
		
	// add the request to the end of the linked list
	curr->val = req1;
	curr->next = NULL;
	curr->prev = NULL;
	if( !s->head ) {
		s->head = curr;
		s->tail = curr;
	}
	else {
		s->tail->next = curr;
		curr->prev = s->tail;
		s->tail = curr;
	}
	//printf("read block %d starting\n",block);
	// wait to be serviced before proceeding
	if( pthread_cond_wait( &cond, &mutex )!= 0 ) {
		fprintf(stderr,"Cond wait failed: %s\n",strerror(errno));
		exit(1);
	}
	
	// unlock the mutex
	if( pthread_mutex_unlock( &mutex )!=0 ) {
		fprintf(stderr,"Mutex unlock failed: %s\n",strerror(errno));
		exit(1);
	}
	//printf("read block %d done\n",block);
}

/*
An infinite loop that waits for requests to be added to the 
data structure, picks the appropriate next request, calls 
disk_read or disk_write as needed, and then wakes up the 
program waiting upon the request.
*/
void * disk_scheduler_run( struct disk_scheduler *s )
{
	while( 1 ) {
		// lock the mutex in the critical section
		if( pthread_mutex_lock( &mutex )!=0 ) {
			fprintf(stderr,"Mutex lock failed: %s\n",strerror(errno));
			exit(1);
		}
		// wait for requests to be added to the data structure
		while( s->head == NULL ) {
			if( pthread_cond_wait( &cond, &mutex )!= 0 ) {
				fprintf(stderr,"Cond wait failed: %s\n",strerror(errno));
				exit(1);
			}
		}

		// pick the appropriate next request
		req *curr;
		
		// just takes the head of the list
		if( s->mode == DISK_SCHEDULER_MODE_FIFO ) {
			curr = s->head;
		}
		
		// finds the request object with the closest
		// block address to nextblock +/- nextdir.
		else if( s->mode == DISK_SCHEDULER_MODE_SSTF ) {
			curr = s->head;
			nextdir = 0;
			while( curr->val->block1 != nextblock+nextdir 
				&& curr->val->block1 != nextblock-nextdir ) {
				curr = curr->next;
				if( curr == NULL ) {
					curr = s->head;
					nextdir++;
					if( nextdir >= disk_scheduler_nblocks( s )-1) {
						nextdir = 0;
					}
				}
			}
			nextblock = curr->val->block1;
		}
		
		// scans back and forth across the disk
		// nextdir = 1 when moving right, -1 when moving left.
		else if( s->mode == DISK_SCHEDULER_MODE_SCAN ) {
			curr = s->head;
			while( curr->val->block1 != nextblock ) {
				curr = curr->next;
				if( curr == NULL ) {
					curr = s->head;
					nextblock += nextdir;
					if( nextblock > disk_scheduler_nblocks( s )-1) {
						nextblock--;
						nextdir = -1;
					}
					if( nextblock < 0 ) {
						nextblock++;
						nextdir = 1;
					}
				}
			}
		}

		// remove the request from the linked list
		if( curr->prev ) {
			curr->prev->next = curr->next;
		}
		if( curr->next ) {
			curr->next->prev = curr->prev;
		}
		if( curr == s->head ) {
			s->head = curr->next;
		}
		if( curr == s->tail ) {
			s->tail = curr->prev;
		}
		
		// exit the critical section, unlock the mutex
		if( pthread_mutex_unlock( &mutex )!=0 ) {
			fprintf(stderr,"Mutex unlock failed: %s\n",strerror(errno));
			exit(1);
		}
		
		// call disk_read or disk_write as needed
		if( curr->val->read_write == REQUEST_WRITE ) {
			disk_write(s->disk,curr->val->block1,curr->val->wdata);
			//printf("Block W%d serviced\n",curr->val->block1);
		}
		else {
			disk_read(s->disk,curr->val->block1,curr->val->rdata);
			//printf("Block R%d serviced\n",curr->val->block1);
		}
		free( curr );
		
		// wake up the program waiting upon the request
		if( pthread_cond_signal( &cond )!=0 ) {
			fprintf(stderr,"Cond signal failed: %s\n",strerror(errno));
			exit(1);
		}
	}
}