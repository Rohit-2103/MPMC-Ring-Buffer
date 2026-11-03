#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#define MAX_SIZE 6
#include <stdbool.h>
#include <pthread.h>

typedef enum
{
    BLOCK,
    DROP_NEWEST,
    DROP_OLDEST
} Overflow_Policy;

typedef struct Ring_Buffer
{

     int arr[MAX_SIZE];
     int head;
     int tail;
   int count;
   pthread_mutex_t mutex;
   pthread_cond_t not_empty;
   pthread_cond_t not_full;
}Ring_Buffer;

void ring_buffer_init(Ring_Buffer *rb);
bool ring_buffer_push(Ring_Buffer *rb,int value,Overflow_Policy policy); 
bool ring_buffer_pop(Ring_Buffer *rb,int *value); 

#endif
