#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <unistd.h>
#include <pthread.h>
#include "fs/operations.h"
#include "tecnicofs-api-constants.h"


/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */


#define MAX_COMMANDS 150000
#define MAX_INPUT_SIZE 100

/* Generic lock structure. Specified when used. */
typedef struct{
    pthread_mutex_t mutex_ref; /* Can be either a mutex lock (...) */
    pthread_rwlock_t rwlock_ref; /* (...) or a r/w lock. */
} pthread_lock_ref;


/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */


/* Locks: for accessing the commands vector and for applying them. */
pthread_lock_ref lock_ref1, lock_ref2;

/* Number of paralel execution threads. */
int numberThreads = 0;

/* Synch strategy: mutex, rwlock or nosync (constants). */
int synch;

char inputCommands[MAX_COMMANDS][MAX_INPUT_SIZE];

int numberCommands = 0;

int headQueue = 0;

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Error function for initializing locks.
 */
void error_init_locks() {
    fprintf(stderr, "Error: Unable to initialize locks.\n");
    exit(EXIT_FAILURE);
}

/*
 * Error function for destroying locks. 
 */
void error_destroy_locks() {
    fprintf(stderr, "Error: Unable to destroy locks.\n");
    exit(EXIT_FAILURE);
}

/*
 * Error function for locking.
 */
void error_lock() {
    fprintf(stderr, "Error: Unable to lock.\n");
    exit(EXIT_FAILURE);
}

/*
 * Error function for unlocking.
 */
void error_unlock() {
    fprintf(stderr, "Error: Unable to unlock.\n");
    exit(EXIT_FAILURE);
}

void errorParse () {
    fprintf(stderr, "Error: command invalid\n");
    exit(EXIT_FAILURE);
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Inicializes all (to be used) locks.
 */
void init_locks () {

    /* Lock for applying the commands. */
    switch(synch) {
        case MUTEX: /* mutex synchstategy plus removeCommand lock*/
            if (pthread_mutex_init(&lock_ref1.mutex_ref, NULL) || 
            pthread_mutex_init(&lock_ref2.mutex_ref, NULL)) {
                error_init_locks(); /* ERROR CASE */
            }
            break;
        case RWLOCK: /* r/w lock synchstrategy plus removeCommand lock*/
            if(pthread_rwlock_init(&lock_ref1.rwlock_ref, NULL) ||
            pthread_mutex_init(&lock_ref2.mutex_ref, NULL)) {
                error_init_locks(); /* ERROR CASE */
            }
            break;
    } 
}

/*
 * Destroys all (used) locks.
 */
void destroy_locks () {

    /* Lock for applying the commands. */
    switch(synch){
        case MUTEX: /* mutex synchstategy */
            if(pthread_mutex_destroy(&lock_ref1.mutex_ref) ||
            pthread_mutex_destroy(&lock_ref2.mutex_ref)) {
                error_destroy_locks(); /* ERROR CASE */
            }
            break;
        case RWLOCK: /* r/w lock synchstrategy */
            if(pthread_rwlock_destroy(&lock_ref1.rwlock_ref) ||
            pthread_mutex_destroy(&lock_ref2.mutex_ref)) {
                error_destroy_locks(); /* ERROR CASE */
            }
            break;
    } 
}

/*
 * Locks a critical section.
 */
void lock (permission mode) {
    switch(synch) {
        case MUTEX: /* mutex synchstategy */
            if(pthread_mutex_lock(&lock_ref1.mutex_ref)) {
                error_lock(); /* ERROR CASE */
            }
            break;
        case RWLOCK: /* r/w lock synchstrategy (...)*/
            if(mode == WRITE) { /* (...) for writing. */
                if(pthread_rwlock_wrlock(&lock_ref1.rwlock_ref)) {
                    error_lock(); /* ERROR CASE */
                }
            }
            else if(mode == READ) { /* (...) for reading. */
                if(pthread_rwlock_rdlock(&lock_ref1.rwlock_ref)) {
                    error_lock(); /* ERROR CASE */
                }
            }
            else { /* ERROR CASE */
                fprintf(stderr, "Error: Invalid mode for rwlock\n");
                exit(EXIT_FAILURE);
            }
            break;
    }
}

/*
 * Unlocks a critical section.
 */
void unlock () {
    switch(synch) {
        case MUTEX: /* mutex synchstategy */
            if(pthread_mutex_unlock(&lock_ref1.mutex_ref)) {
                error_unlock(); /* ERROR CASE */
            }
            break;
        case RWLOCK: /* r/w lock synchstrategy (...)*/
            if(pthread_rwlock_unlock(&lock_ref1.rwlock_ref)) {
                error_unlock(); /* ERROR CASE */
            }
            break;
    }
}

/*
 * Defines the synchstrategy
 */
void syncmode (char * sync) {
    if (!strcmp(sync,"mutex")) { /* mutex synchstategy */
        synch = MUTEX;
    }
    else if (!strcmp(sync,"rwlock")) { /* r/w lock synchstrategy */
        synch = RWLOCK;
    }
    else if (!strcmp(sync,"nosync")) { /* no synchstrategy */
        if (numberThreads != 1) { /* ERROR CASE */
            printf("Error: nosync only allowed with one thread.\n");
            exit(EXIT_FAILURE);
        }
        synch = NOSYNC;
    }
    else { /* ERROR CASE */
        printf("Error: unknown sync strategy.\n");
        exit(EXIT_FAILURE);
    }
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Creating the threads vector.
 */
pthread_t * create_threads_vec(char * numT) {
    pthread_t * tid;

    if((numberThreads = * numT - '0') < 1){ /* ERROR CASE */
        fprintf(stderr, "Error: Invalid number of threads.\n");
        exit(EXIT_FAILURE);
    }

    /* create the vector */
    if(!(tid = (pthread_t *) malloc(numberThreads * sizeof(pthread_t)))){
        fprintf(stderr, "Error: No memory allocated for threads.\n");
        exit(EXIT_FAILURE);
    }   

    return tid;
}

int insertCommand(char* data) {

    if(numberCommands != MAX_COMMANDS) {
        strcpy(inputCommands[numberCommands++], data);
        return 1;
    }

    return 0;
}

char * removeCommand() {

    if(numberCommands > 0){
        numberCommands--;
        return inputCommands[headQueue++];  
    }
    
    return NULL;
}

void processInput(char * inputfile) {
    char line[MAX_INPUT_SIZE];
    FILE* file;
    
    /* open the input file (...) */
    if(!(file = fopen(inputfile, "r"))) {
        perror("Could not open the input file");
        exit(EXIT_FAILURE);
    }

    /* (...) process its information (...) */
    /* break loop with ^Z or ^D */
    while (fgets(line, sizeof(line)/sizeof(char), file)) {
        char token, type;
        char name[MAX_INPUT_SIZE];

        int numTokens = sscanf(line, "%c %s %c", &token, name, &type);

        /* perform minimal validation */
        if (numTokens < 1) {
            continue;
        }
        switch (token) {
            case 'c':
                if(numTokens != 3)
                    errorParse();
                if(insertCommand(line))
                    break;
                return;
            
            case 'l':
                if(numTokens != 2)
                    errorParse();
                if(insertCommand(line))
                    break;
                return;
            
            case 'd':
                if(numTokens != 2)
                    errorParse();
                if(insertCommand(line))
                    break;
                return;
            
            case '#':
                break;
            
            default: { /* error */
                errorParse();
            }
        }
    }
    
    /* (...) close the input file */
    if (fclose(file)) {
        perror("Could not close the input file");
        exit(EXIT_FAILURE);
    }
}

void processOutput(char * outputfile) {
    FILE * file;

    /* open the output file*/
    if(!(file = fopen(outputfile, "w"))){
        perror("Could not open the output file");
        exit(EXIT_FAILURE);
    }

    /* print the fs tree to the outputfile*/
    print_tecnicofs_tree(file);

    /* close the output file */
    if (fclose(file)) {
        perror("Could not close the input file");
        exit(EXIT_FAILURE);
    }
}

void applyCommands(){
    while (numberCommands > 0){
        
        /* lock when using multiple threads*/
        if(synch != NOSYNC)
            if (pthread_mutex_lock(&lock_ref2.mutex_ref)) { /* LOCK */
                error_lock(); /* ERROR CASE */
            }

        const char* command = removeCommand();

        /* unlock when using multiple threads*/
        if(synch != NOSYNC)
            if (pthread_mutex_unlock(&lock_ref2.mutex_ref)) {/* UNLOCK */
                error_unlock(); /* ERROR CASE */
            }

        if (command == NULL){
            continue;
        }

        char token, type;
        char name[MAX_INPUT_SIZE];
        int numTokens = sscanf(command, "%c %s %c", &token, name, &type);
        if (numTokens < 2) {
            fprintf(stderr, "Error: invalid command in Queue\n");
            exit(EXIT_FAILURE);
        }

        int searchResult;
        switch (token) {

            case 'c': /* critical section -> writing */
                lock(WRITE); /* LOCK */
                switch (type) {
                    case 'f':
                        printf("Create file: %s\n", name);
                        create(name, T_FILE);
                        break;
                    case 'd':
                        printf("Create directory: %s\n", name);
                        create(name, T_DIRECTORY);
                        break;
                    default:
                        fprintf(stderr, "Error: invalid node type\n");
                        exit(EXIT_FAILURE);
                }
                unlock(WRITE); /* UNLOCK */
                break;

            case 'l': /* critical section -> reading */
                lock(READ);/* LOCK */
                searchResult = lookup(name);
                if (searchResult >= 0) {
                    printf("Search: %s found\n", name);
                }
                else {
                    printf("Search: %s not found\n", name);
                }
                unlock(READ); /* UNLOCK */
                break;

            case 'd': /* critical section -> writing */
                lock(WRITE); /* LOCK */
                printf("Delete: %s\n", name);
                delete(name);
                unlock(WRITE); /* UNLOCK */
                break;

            default: { /* error */
                fprintf(stderr, "Error: command to apply\n");
                exit(EXIT_FAILURE);
            }
        }
    }
}

/*
 * Auxiliary function for using applyCommands with threads.
 */
void * fnThread(void * arg) {
    applyCommands();
    return NULL;
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

int main(int argc, char* argv[]) {

    int numT;
    struct timeval  tv1, tv2;
    pthread_t * tid;

    if(argc != 5){
        fprintf(stderr, "Expected Format: ./tecnicofs <inputfile> <outputfile> <numberThread> <synch mode>\n");
        exit(EXIT_FAILURE);
    }
        

    /* create the threads vector */
    tid = create_threads_vec(argv[3]);  

    /* define the syncmode */
    syncmode(argv[4]);

    /* init filesystem */
    init_fs();

    /* process input */
    processInput(argv[1]);

    /* init locks */
    init_locks();
    
    /* create the execution threads */
    for (numT = 0; numT < numberThreads; numT++) {
        if (pthread_create(&tid[numT], NULL, fnThread, NULL) != 0) {
            exit(EXIT_FAILURE);
        }
    }
    
    /* measuring the execution time (begin time) */
    gettimeofday(&tv1, NULL);

    /* waiting for all the threads to finish */
    for (numT = 0; numT < numberThreads; numT++) {
        if (pthread_join(tid[numT], NULL) != 0) {
            exit(EXIT_FAILURE);
        }
    }

    /* measuring the execution time (end time) */
    gettimeofday(&tv2, NULL);

    /* calculating the execution time */
    printf ("TecnicoFS completed in %.4f seconds.\n",
         (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
         (double) (tv2.tv_sec - tv1.tv_sec));

    /* process output (results, final fs) */
    processOutput(argv[2]);
    
    /* release allocated memory */
    free(tid);
    destroy_locks();
    destroy_fs();

    exit(EXIT_SUCCESS);
}

