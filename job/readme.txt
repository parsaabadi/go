This directory is an example of models run job directories structure:

job/        : this folder must be shared bteween all oms instances
    active/   : active model runs state
    history/  : completed (success or fail) model runs
    queue/    : model runs queue
    state/    : servers state and, jobs state and oms instances state
          jobs.queue.paused : if this file exist the model run queue is paused
    job.ini   : job control settings

To use model run jobs use -oms.JobDir option, for example:

cd openmpp-root-dir
bin/oms -oms.JobDir job
