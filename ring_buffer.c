
#include<stdio.h>
#include "ring_buffer.h"


void ring_buffer_init(Ring_Buffer *rb)
{
    rb->head=0;
    rb->tail=0;
    rb->count=0;
    pthread_mutex_init(&rb->mutex,NULL);
    pthread_cond_init(&rb->not_empty,NULL);
    pthread_cond_init(&rb->not_full,NULL);
}

bool ring_buffer_push(Ring_Buffer *rb,int value,Overflow_Policy policy)
{
   
     if(rb->count==MAX_SIZE)
     {
        if(policy==DROP_NEWEST)
        {
        return 0;
        }
        else if(policy==DROP_OLDEST)
        {
            rb->tail=(rb->tail+1)%MAX_SIZE;
            rb->arr[rb->head]=value;
            rb->head=(rb->head+1)%MAX_SIZE;
            return 1;
        }
     }
  
     rb->arr[rb->head]=value;

     rb->head = (rb->head + 1) % MAX_SIZE;

     rb->count++;
   
    return 1;
    
}

bool ring_buffer_pop(Ring_Buffer *rb,int *value)
{
    if(rb->count==0)
    {
        return 0;
    }

    *value=rb->arr[rb->tail];
    rb->tail=(rb->tail+1)% MAX_SIZE;
    rb->count--;
    return 1;

}