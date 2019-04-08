# clas12-workflow

## Overview
Wrapper tools for [JLab's Swif](https://scicomp.jlab.org/docs/swif).

Initially motivated by mass-decoding of CLAS12 Spring RGA data, whose requirements include:
* Decode single EVIO files, independently (to optimize tape access)
* Merge M sequential HIPO files into 1 HIPO file
* Write merged HIPO files to tape, in sequence (unlike the raw EVIO files)
* Maintain a fixed and available disk space requirement

## CLAS12 Decoding Workflows

Note, Swif phase `i+1` doesn't start until phase `i` has succesfully finished.  Each phase's decoding jobs are automatically limited to a single run number.  These workflows are subclasses of a generic SwifWorkflow.

### _3-phase_:
Requires N GB of temporary disk space.  Only every third phase reads from tape. 
* Phase 1
  * N jobs
  * decode N EVIO files into N HIPO files
* Phase 2
  * N/M jobs
  * merge N HIPO files into N/M HIPO files (M->1)
* Phase 3
  * 2 jobs
  * delete N HIPO files
  * move N/M HIPO files to destination
* Repeat phases 1+2+3 until all requested runs/files are exhausted ...

### _Rolling_:
Requires 3N GB of temporary disk space.  Every phase reads from tape.
* Phase 0
  * N jobs
  * decode N EVIO files
* Phase 1
  * N + N/M jobs
  * decode next N EVIO files
  * merge previous phase's N HIPO files into N/M HIPO files
* Phase 2
  * N + N/M + 2 jobs
  * decode next N EVIO files
  * merge previous phase's N HIPO files into N/M HIPO files
  * delete previous-previous phase's N HIPO files
  * move previous-previous phase's N/M HIPO files to destination
* Repeat phase 2 until all requested runs/files are exhausted ...

### _SinglesOnly_:
Just decodes single files, with independent cronjob to merge and move.

## Usage

### Generating CLAS12 workflows

First, setup the environment:

`source ./env.csh`

These examples will generate a Swif workflow and write it to files in `./jobs`, _using the default configuration_, for a single decoding workflow for run 4013 and 4014, all runs between 4000 and 4200, or for a list of runs read from a file:

`./scripts/gen-decoding.py --runGroup rga --workflow decodeA --runs 4013,4014`

`./scripts/gen-decoding.py --runGroup rga --workflow decodeB --runs 4000-4200`

`./scripts/gen-decoding.py --runGroup rga --workflow decodeC --runs filename`

After importing the resulting file in `./jobs` (via `swif import -slurm -file filename`, currently the default is PBS without the -slurm option), you would `swif run` it to start the workflow.

* You'll need to modify the default configuration
  * all necessary settings are available from the command line, run it with the `-h` option to see
* N (`phaseSize`) should be significantly larger than M (`mergeSize`) to allow Swif to optimize tape access
* N should be evenly divisible by M, else you'll always get one smaller merge file per phase and irregular merged file numberings

### Monitoring / Control

`./scripts/swif-status.py` wraps various Swif commands.  By default just prints all current workflows' statuses, with command-line options to:
* save the current status, accumulating log, and full job details to log files, and publish to web directory
* automatically retry any problem jobs, and increase resource requests if necessary
* relocate `~/.farm_out` logs at end of workflow

See `./cron/swif.cron` for an example cron job, where retry attempts will cause cron to generate an automatic email.

## Details

### Features
* intended to be reusable in the future (see `lib/Swif*` classes)
* automatically overrides Swif/Auger's symlinking `/cache` files to the batch node with a `dd bs=1M` copy.
* uses JSON Swif configs
  * much faster to create workflows and provides a bit more control (e.g. job names) than `swif add-job`
* log files
  * named as job name appended with job tags (for easier association)
  * write job logs to configureable directory path (i.e. not `~/.farm_out`)
  * automatically move remaining `~/.farm_out` log files at end of workflow
* CLAS12 workflows
  * file integrity checks during the jobs, based on return value of `hipo-utils -test`
  * retrieve torus/solenoid scales from RCDB during workflow generation (overridable from command line)
  * utilize Swif's job tags (e.g. output directory, run/file numbers, coatjava version)
  * automatically retries jobs due to system failures and adjusts job resource reqs if necessary
  * periodically write workflow status to clas12mon, for timeline plots and easy global status
    * https://clas12mon.jlab.org/status/decoding/

### CLAS12 Decoding Lessons Learned
* the single, largest, and consistent bottleneck is reading from tape silo
* _Rolling_ workflow averages around 7500 files per day for RGA Spring 2018, seen up to 12K/day
* normal failure rate due to batch system is up to 10% (instantaneous, average is much lower)
  * inisignificant effect on throughput due to tape bottleneck
  * all recoverable with a retry, or in rare cases increase job ram/time (all automated)
    * ultimately, so far, success rate is 100% without human intervention
  * common problems with nodes
    * no space available on local filesystem 
    * cannot find input files (lustre or /work filesystems)
    * cannot find basic system commands (e.g. rm!)
  * flurry of corrupt hipo files
    * presumably due to system problems (since irreproducible) 
    * added integrity checks before allowing output files to be written

