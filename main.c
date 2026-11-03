
#include<stdio.h>
#include <pthread.h>
#include "ring_buffer.h"

#include <windows.h>

// pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
// pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;
// pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;

typedef struct
{
   Ring_Buffer *rb;
   int start_value;
   Overflow_Policy policy;
}Producer_Args;

typedef struct
{
    Ring_Buffer *rb;
    int consumer_id;
} Consumer_Args;


void *producer(void *arg)
{
    
    Producer_Args *args=(Producer_Args*)arg;
    for(int i=0;i<5;i++)
   { 
    pthread_mutex_lock(&args->rb->mutex);
    if(args->policy==BLOCK)
    {
    while(args->rb->count==MAX_SIZE)
    {
        printf("Producer blocked: buffer is full\n");
        pthread_cond_wait(&args->rb->not_full,&args->rb->mutex);
        //printf("Producer woke up; count = %d\n", rb->count);
    }
     }
    bool success=ring_buffer_push(args->rb,args->start_value+i*10,args->policy);
    if(success)
    {
    printf("PUSHED: %d\n", args->start_value + i * 10);
    pthread_cond_signal(&args->rb->not_empty);
    }
    else
    printf("The value was not pushed in buffer\n");
    
    pthread_mutex_unlock(&args->rb->mutex);
    //Sleep(100);
}
return NULL;
}
void *consumer(void *arg)
{
  int removed_value=-1;
  //Ring_Buffer *rb =(Ring_Buffer *)arg;

  Consumer_Args *args = (Consumer_Args *)arg;
   Ring_Buffer *rb = args->rb;
  
  for(int i=0;i<5;i++)
  {
    pthread_mutex_lock(&rb->mutex);
    while(rb->count==0)
    {
         pthread_cond_wait(&rb->not_empty,&rb->mutex);
         printf("Consumer %d waiting: buffer is empty\n",args->consumer_id);
    }
    ring_buffer_pop(rb,&removed_value); 
    //printf("Consumer removed an item; count = %d\n", rb->count);
    // printf("Value removed was %d\n",removed_value);
    printf("Consumer %d removed: %d\n",args->consumer_id, removed_value);
    pthread_cond_signal(&rb->not_full);
    pthread_mutex_unlock(&rb->mutex);

  }
  return NULL;
}
int main()
{

    Ring_Buffer rb;
    
    // Initialize the ring buffer
    ring_buffer_init(&rb);
    if(rb.head==0)
    printf("The list is initialized safetly\n");
    pthread_t producer_thread;
    pthread_t consumer_thread;
    pthread_t producer_thread2;
    pthread_t consumer_thread2;
    
    Producer_Args args1;
    Producer_Args args2;

    Consumer_Args cargs1;
    Consumer_Args cargs2;

    args1.rb=&rb;
    args1.start_value=10;
    args1.policy=BLOCK;

    args2.rb=&rb;
    args2.start_value=60;
    args2.policy=BLOCK;

    cargs1.rb = &rb;
    cargs1.consumer_id = 1;
    cargs2.rb = &rb;
    cargs2.consumer_id = 2;

    
    if(pthread_create(&producer_thread,NULL,producer,&args1)!=0)
    {
        printf("Producer creation failed\n");
        return 1;
    }
    if(pthread_create(&producer_thread2,NULL,producer,&args2)!=0)
    {
        printf("Producer 2 creation failed\n");
        return 1;
    }
    if(pthread_create(&consumer_thread,NULL,consumer,&cargs1)!=0)
    {
        printf("Consumer creation failed\n");
        return 1;
    }
    if(pthread_create(&consumer_thread2,NULL,consumer,&cargs2)!=0)
    {
        printf("Consumer 2 creation failed\n");
        return 1;
    }
     if(pthread_join(consumer_thread,NULL)!=0)
    {
        printf("Consumer thread join failed\n");
        return 1;
    }
    if(pthread_join(consumer_thread2,NULL)!=0)
    {
        printf("Consumer thread 2 join failed\n");
        return 1;
    }
     if(pthread_join(producer_thread,NULL)!=0)
    {
        printf("Producer thread join failed\n");
        return 1;
    }
    if(pthread_join(producer_thread2,NULL)!=0)
    {
        printf("Producer 2 thread join failed\n");
        return 1;
    }

   

    printf("\n");
    pthread_cond_destroy(&rb.not_empty);
    pthread_cond_destroy(&rb.not_full);
    pthread_mutex_destroy(&rb.mutex);

  

    return 0;
}