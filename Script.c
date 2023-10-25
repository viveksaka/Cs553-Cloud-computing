#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/stat.h>

#define USAGE "./external_sort <input file> <output file> <number of threads>"
#define BUFFER_SIZE 100

struct stat file_info;
char **data;
int CHUNK_SIZE;
int NUM_THREADS;
int REMAINDER;

void merge(char *arr[], int left, int middle, int right);
void merge_sort(char *arr[], int left, int right);

void *thread_merge(void* arg) {
    int thread_id = (long)arg;
    int start = thread_id * CHUNK_SIZE;
    int end = (thread_id + 1) * CHUNK_SIZE - 1;
    if (thread_id == NUM_THREADS - 1) {
        end += REMAINDER;
    }
    int middle = start + (end - start) / 2;
    if (start < end) {
        merge_sort(data, start, end);
        merge_sort(data, start + 1, end);
        merge(data, start, middle, end);
    }
    return NULL;
}

void merge_chunks(char *arr[], int count, int step, int total_length, int CHUNK_SIZE) {
    for (int i = 0; i < count; i = i + 2) {
        int left = i * (CHUNK_SIZE * step);
        int right = ((i + 2) * CHUNK_SIZE * step) - 1;
        int middle = left + (CHUNK_SIZE * step) - 1;
        if (right >= total_length) {
            right = total_length - 1;
        }
        merge(arr, left, middle, right);
    }
    if (count / 2 >= 1) {
        merge_chunks(arr, count / 2, step * 2, total_length, CHUNK_SIZE);
    }
}

void merge(char *arr[], int left, int middle, int right) {
    int left_length = middle - left + 1;
    int right_length = right - middle;
    char **left_arr = calloc(left_length, sizeof(char*));
    char **right_arr = calloc(right_length, sizeof(char*));
    
    for (int i = 0; i < left_length; i++) {
        left_arr[i] = arr[left + i];
    }
    
    for (int j = 0; j < right_length; j++) {
        right_arr[j] = arr[middle + 1 + j];
    }
    
    int i = 0;
    int j = 0;
    int k = 0;

    while (i < left_length && j < right_length) {
        if (*left_arr[i] < *right_arr[j]) {
            arr[left + k] = left_arr[i];
            i++;
        }
        else if (*left_arr[i] == *right_arr[j]) {
            for (int z = 1; z < 10; z++) {
                if (*(left_arr[i] + z) == *(right_arr[j] + z)) {
                    continue;
                }
                else if (*(left_arr[i] + z) < *(right_arr[j] + z)) {
                    arr[left + k] = left_arr[i];
                    i++;
                    break;
                }
                else {
                    arr[left + k] = right_arr[j];
                    j++;
                    break;
                }
            }
        }
        else {
            arr[left + k] = right_arr[j];
            j++;
        }
        k++;
    }
    
    while (i < left_length) {
        arr[left + k] = left_arr[i];
        k++;
        i++;
    }
    while (j < right_length) {
        arr[left + k] = right_arr[j];
        k++;
        j++;
    }
}

void merge_sort(char *arr[], int left, int right) {
    if (left < right) {
        int middle = left + (right - left) / 2;
        merge_sort(arr, left, middle);
        merge_sort(arr, middle + 1, right);
        merge(arr, left, middle, right);
    }
}

void external_sort(char* input_file, char* output_file, int num_threads) {
    char* buffer;
    int i = 0;
    FILE* input_file_ptr;
    FILE* output_file_ptr;

    stat(input_file, &file_info);
    long int file_length = file_info.st_size / 100;

    data = calloc(file_length, sizeof(char*));

    input_file_ptr = fopen(input_file, "r");
    if (input_file_ptr == NULL) {
        fprintf(stderr, "Error opening %s", input_file);
        return;
    }

    output_file_ptr = fopen(output_file, "w");
    if (output_file_ptr == NULL) {
        fprintf(stderr, "Error opening %s", output_file);
        return;
    }

    buffer = (char*) malloc(sizeof(char) * BUFFER_SIZE);

    while (fread(&buffer[0], sizeof(char), BUFFER_SIZE, input_file_ptr) == BUFFER_SIZE) {
        data[i] = calloc(BUFFER_SIZE, sizeof(char));
        for (int j = 0; j < 98; j++) {
            data[i][j] = buffer[j];
        }
        i++;
    }

    REMAINDER = file_length % num_threads;
    CHUNK_SIZE = file_length / num_threads;
    NUM_THREADS = num_threads;

    pthread_t threads[num_threads];

    for (long thread_id = 0; thread_id < num_threads; thread_id++) {
        int rc = pthread_create(&threads[thread_id], NULL, thread_merge, (void *)thread_id);

        if (rc) {
            printf("Error: %d\n", rc);
            exit(-1);
        }
    }

    for (int thread_id = 0; thread_id < num_threads; thread_id++) {
        pthread_join(threads[thread_id], NULL);
    }

    merge_chunks(data, num_threads, 1, file_length, CHUNK_SIZE);

    for (i = 0; i < file_length; i++) {
        for (int j = 0; j < 99; j++) {
            buffer[j] = data[i][j];
        }
        fwrite(&buffer[0], sizeof(char), BUFFER_SIZE, output_file_ptr);
    }

    free(buffer);
    fclose(output_file_ptr);
    fclose(input_file_ptr);
}

int main(int argc, char** argv) {
    char* input_file;
    char* output_file;
    int num_threads;
    struct timeval start, end;
    double execution_time;

    if (argc != 4) {
        fprintf(stderr, USAGE);
        return 1;
    }

    input_file = argv[1];
    output_file = argv[2];
    num_threads = atoi(argv[3]);

    gettimeofday(&start, NULL);
    external_sort(input_file, output_file, num_threads);
    gettimeofday(&end, NULL);
    execution_time = ((double) end.tv_sec - (double) start.tv_sec) + ((double) end.tv_usec - (double) start.tv_usec) / 1000000.0;
    
    printf("input file: %s\n", input_file);
    printf("output file: %s\n", output_file);
    printf("number of threads: %d\n", num_threads);
    printf("execution time: %lf\n", execution_time);

    return 0;
}

    
