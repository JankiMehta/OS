#include "program.h"
#include "disk_scheduler.h"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>


void * program_run( struct disk_scheduler *s )
{
	int nblocks = disk_scheduler_nblocks(s);
	char data[4096];
	int i;

	for(i=0;i<100;i++) {
		int seed;

		int readorwrite = rand_r(&seed) % 2;
		int block = rand_r(&seed) % nblocks;

		if(readorwrite) {
			disk_scheduler_read(s,block,data);
		} else {
			disk_scheduler_write(s,block,data);
		}
	}

	return 0;
}