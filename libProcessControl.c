#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<fcntl.h>
#include<unistd.h>
#include<signal.h>
#include<sys/types.h>
#include<sys/wait.h>
#include<sys/stat.h>

#include "libParseArgs.h"
#include "libProcessControl.h"
#define MAX 1024

/**
 * parallelDo -n NUM -o OUTPUT_DIR COMMAND_TEMPLATE ::: [ ARGUMENT_LIST ...]
 * build and execute shell command lines in parallel
 */

/**
 * Return the number of {} found in commandTemplate
 */
int count_holders(char *commandTemplate, int len){
    int count = 0;
    for (int i = 0; i < len-1; i++){
        if (commandTemplate[i] == '{' && commandTemplate[i+1] == '}'){
            count++;
        }
    }
    return count;
}
/**
 * Return the length of a string when all occurences of {} in
 * commandTemplate is substituted with argument
 */
int get_command_len(char *commandTemplate, char *argument){
    int s_len = strnlen(commandTemplate, MAX);
    int arg_len = strnlen(argument, MAX);
    int num_holder = count_holders(commandTemplate, s_len);
    int command_len = s_len - (num_holder * 2);
    command_len += (num_holder * arg_len) + 1;
    return command_len;
}
/**
 * create and return a newly malloced command from commandTemplate and argument
 * the new command replaces each occurrance of {} in commandTemplate with argument
 */
char *createCommand(char *commandTemplate, char *argument){
	char *command=NULL;
    // Setting up to create the command
    int command_len = get_command_len(commandTemplate, argument);
    command = malloc(command_len * sizeof(char));
	if (command == NULL){
        perror("malloc");
        exit(1);
    }
    // Go through each char in commandTemplate and add it to command
    // if {} found - replace it with argument into command and move on
    int j = 0;
    int i = 0;
    while (i < strnlen(commandTemplate, MAX)){
        if (commandTemplate[i] == '{' && commandTemplate[i+1] == '}'){
            int k = 0;
            while (k < strnlen(argument, MAX)){
                command[j] = argument[k];
                k++;
                j++;
            }
            i += 2;
        }
        else{
            command[j] = commandTemplate[i];
            i++;
            j++;
        }
    }
    command[j] = '\0';
	return command;
}

typedef struct PROCESS_STRUCT {
	int pid;
	int ifExited;
	int exitStatus;
	int status;
	char *command;
} PROCESS_STRUCT;

typedef struct PROCESS_CONTROL {
	int numProcesses;
	int numRunning; 
	int maxNumRunning;
	int numCompleted;
	PROCESS_STRUCT *process;
} PROCESS_CONTROL;

PROCESS_CONTROL processControl;

void printSummary(){
	printf("%d %d %d\n", processControl.numProcesses, processControl.numCompleted, processControl.numRunning);
}
void printSummaryFull(){
        printSummary();
        int i=0, numPrinted=0;
        while(numPrinted<processControl.numCompleted && i<processControl.numProcesses){
                if(processControl.process[i].ifExited){
                        printf("%d %d %d %s\n",
                                processControl.process[i].pid,
                                processControl.process[i].ifExited,
                                processControl.process[i].exitStatus,
                                processControl.process[i].command);
                        numPrinted++;
                }
                i++;
        }
}
/**
 * find the record for pid and update it based on status
 * status has information encoded in it, you will have to extract it
 */
void updateStatus(int pid, int status){
    processControl.process[processControl.numCompleted].exitStatus = WEXITSTATUS(status);
    processControl.process[processControl.numCompleted].pid = pid;
    processControl.process[processControl.numCompleted].status = status;
    processControl.process[processControl.numCompleted].ifExited = WIFEXITED(status);
    processControl.numCompleted += 1;
    processControl.numRunning -= 1;
    // Decreasing the number of processes allowed to run if there are less
    // commands left to execute than there are allowed processes
    if (pparams.maxNumRunning > (pparams.argumentListLen - processControl.numCompleted)){
        pparams.maxNumRunning--;
    }
	
}

void handler(int signum){
    if (signum == SIGUSR1){
        printSummary();
    }
    else {
        printSummaryFull();
    }
}
/**
 * Return a string in the format of outdir/fname
*/
char *createDir(char *outdir, char *fname){
    int outlen = strnlen(outdir, MAX);
    int flen = strnlen(fname, MAX);
    char *slash = "/";
    char *dir = NULL;
    dir = malloc((outlen + flen + 2) * sizeof(char));
    if (dir == NULL){
        perror("malloc");
    }
    // Start off with outdir, then add /, then add filename
    strncpy(dir, outdir, outlen);
    strncat(dir, slash, 1);
    strncat(dir, fname, flen);
    return dir;
}
/**
 * Given an OUTPUT_DIR, PID, and out return a file descriptor
 * that points to OUTPUT_DIR/PID.stdout or OUTPUT_DIR/PID.stderr
 * Precondition: out is either .stdout or .stderr
*/
int getfd(char *outdir, int pid, char *out){
    // Assuming pid will not be an integer with over 20 places
    char *pid_str = malloc(20 * sizeof(char));
    sprintf(pid_str, "%d", pid);
    // Length of .stdout and .stderr is 7, add one for /0
    char *fname = malloc(strnlen(pid_str, 20) + 8);
    if (fname == NULL){
        perror("malloc");
        exit(1);
    }
    strncpy(fname, pid_str, 20);
    strncat(fname, out, 7);
    char *dir = createDir(outdir, fname);
    free(fname);
    // Creating the file if it does not exist
    FILE *fp;
    fp  = fopen (dir, "w");
    fclose(fp);
    // fd points to dir for writing
    int fd;
    if ((fd=open(dir, O_WRONLY|O_CREAT|O_TRUNC)) < 0) {
			perror(out);
			return(1);
	}
    return fd;
}
/**
 * This function does the bulk of the work for parallelDo. This is called
 * after understanding the command line arguments. runParallel 
 * uses pparams to generate the commands (createCommand), 
 * forking, redirecting stdout and stderr, waiting for children, ...
 * Instead of passing around variables, we make use of globals pparams and
 * processControl. 
 */
int runParallel(){
    // Creating OUT_DIR in case it does not exist
    char *outdir = pparams.outputDir;
    mkdir(outdir, 0777);

    signal(SIGUSR1, handler);
    signal(SIGUSR2, handler);
    
    processControl.maxNumRunning = pparams.maxNumRunning;
    processControl.numCompleted = 0;
    processControl.numProcesses = 0;
    processControl.numRunning = 0;
    processControl.process = malloc(pparams.argumentListLen * sizeof(PROCESS_STRUCT));
    if (processControl.process == NULL){
        perror("malloc");
        exit(1);
    }

    int i = 0;
    while (i < pparams.argumentListLen){
        if (processControl.maxNumRunning <= processControl.numRunning){
            int status;
            int pid = wait(&status);
            updateStatus(pid, status);
        }
        if (processControl.maxNumRunning > processControl.numRunning){
            processControl.numRunning += 1;
            processControl.numProcesses += 1;
            int pid = fork();
            char *command = createCommand(pparams.commandTemplate, pparams.argumentList[i]);
            processControl.process[i].command = command;
            if (pid == 0){
                int mypid = getpid();
                int fdout = getfd(outdir, mypid, ".stdout");
                int fderr = getfd(outdir, mypid, ".stderr");
                dup2(fdout, 1);
                dup2(fderr, 2);
                close(fdout);
                close(fderr);
                int status;
                wait(&status);
                updateStatus(mypid, status);
                execl("/bin/bash", "/bin/bash", "-c", command, (char *)NULL);
                perror("execl");
                exit(1);
            }
            else{
                int status;
                wait(&status);
                updateStatus(pid, status);
            }
        }
        i++;
    }
    
	
	printSummaryFull();
}
