# clas12-workflow

## Overview
Wrapper tools for [JLab's Swif](https://scicomp.jlab.org/docs/swif) with full implementation of CLAS12 offline workflows.

## Details
`clas12-workflow.py` is the user interface for workflow generation, and its `-h` option will print the help information below.

All options are not required for all models, but clear feedback is given on missing but required options, and warnings are given on ignored options.  So the _`clas12-workflow.py` command-line can be used to progressively build a proper workflow without needing to know everything upfront_.

Note that the `--config` parameter provides for loading options from a file, and the `--show` option prints the current configuration in a format suitable for the `--config` input.  In all cases, command-line options override the `--config` file.

**_Two key parameters control the overall workflow, `--model` and `--phaseSize`:_**

* **_Model_** describes the different tasks that will be performed (e.g. decoding, merging, reconstruction, analysis trains).  For a given input EVIO/HIPO file, these tasks will be performed in a series of jobs.  Those jobs are submitted to the farm by Swif when the appropriate dependencies are satisfied, which is determined by the phase size.
* **_Phase size_** affects how dependencies within the workflow are managed.  Two dependency styles are available: _job-job dependencies_ and _phased dependencies_.  The former is the default, denoted by a negative `--phaseSize`, and maximizes concurrent batch farm footprint.  The latter groups jobs into Swif phases by run number (with finer segmentation if `--phaseSize` is greater than zero), which provides a more throttled workflow (e.g. suitable for running other workflows simultaneously by the same user) and more ordering of the output files on tape.

**_And a couple peculiarities:_**
* For workflows that include decoding, `--decDir` allows to send the decoded files to a different destination than the other tasks (since decoding is unique in that it is generally only done once).
* For workflows that include decoding, merging _and_ phase dependencies, setting `--workDir` enables staging of single decoded files before merging (otherwise decoding jobs are many-to-one file I/O).


```
ifarm1801> clas12-workflow.py -h

usage: clas12-workflow.py [-h] [--runGroup NAME] [--tag NAME] [--model NAME]
                          [--inputs PATH] [--runs RUN/PATH] [--outDir PATH]
                          [--decDir PATH] [--workDir PATH] [--logDir PATH]
                          [--coatjava PATH] [--clara PATH] [--threads #]
                          [--reconYaml PATH] [--trainYaml PATH]
                          [--claraLogDir PATH] [--phaseSize #] [--mergeSize #]
                          [--trainSize #] [--torus #.#] [--solenoid #.#]
                          [--fileRegex REGEX] [--config PATH] [--defaults]
                          [--show] [--submit] [--version]

Generate a CLAS12 SWIF workflow.

optional arguments:
  -h, --help          show this help message and exit
  --runGroup NAME     (*) run group name
  --tag NAME          (*) workflow name suffix/tag, e.g. v0, automatically
                      prefixed with runGroup and task to define workflow name
  --model NAME        (*) workflow model (dec/decmrg/rec/ana/decrec/decmrgrec/
                      recana/decrecana/decmrgrecana)
  --inputs PATH       (*) name of file containing a list of input files, or a
                      directory to be searched recursively for input files, or
                      a shell glob of either. This option is repeatable.
  --runs RUN/PATH     (*) run numbers (e.g. "4013" or "4013,4015" or
                      "3980,4000-4999"), or a file containing a list of run
                      numbers. This option is repeatable.
  --outDir PATH       final data location
  --decDir PATH       overrides outDir for decoding
  --workDir PATH      temporary data location (for merging and phased
                      workflows only)
  --logDir PATH       log location (otherwise the SLURM default)
  --coatjava PATH     coatjava install location
  --clara PATH        clara install location
  --threads #         number of Clara threads
  --reconYaml PATH    recon yaml file
  --trainYaml PATH    train yaml file
  --claraLogDir PATH  location for clara log files
  --phaseSize #       number of files per phase (negative is unphased)
  --mergeSize #       number of files per merge
  --trainSize #       number of files per train
  --torus #.#         override RCDB torus scale
  --solenoid #.#      override RCDB solenoid scale
  --fileRegex REGEX   input filename format (for matching run and file
                      numbers)
  --config PATH       load config file (overriden by command line arguments)
  --defaults          print default config file and exit
  --show              print config file and exit
  --submit            submit and run jobs immediately
  --version           show program's version number and exit

(*) = required option for all workflows, from command-line or config file
```

### Examples

First, setup the environment:

`module load workflow` on a CUE machine (or `source $CLAS12WFLOW/env.[c]sh` for a local install)

Then this would generate a workflow for singles decoding of runs 4013 and 4014, and write its JSON file to the current working directory for inspection:

`clas12-workflow.py --runGroup rga --model dec --runs 4013,4014 --inputs /mss/clas12/rg-a/data --outDir /volatile/clas12/rg-a/test`

You could alternatively specify `--runs 4000-4200` or `--runs filename` to do all runs between 4000 and 4200 or read the runs from a file, respectively.

To also merge the decoded files, change the `--model` option to `decmrg`, to add reconstruction `decmrgrec`, or for just analysis trains `ana`, etc.  The available models are shown with the `-h` option.

After importing the resulting JSON file via `swif import -file filename`, you would `swif run` it to start the workflow.  Alternatively, the `--submit` option would have executed these two Swif steps automatically.

See the `$CLAS12WFLOW/examples` directory for example command-lines and config files. 

### Monitoring / Control
`swif-status.py` wraps various Swif commands.  By default it just prints all your current workflows' statuses, with command-line options to:
* retry any problem jobs and automatically increase their resource requests if necessary
* push status to clas12mon for timelines
* save the current status and full job details, and publish to a web directory

See `$CLAS12WFLOW/cron/swif.cron` for an example cron job.

## Features
* intended to be extendable to non-CLAS12 workflows, see `lib/swif/` and `lib/util/`
* automatically overrides the batch system's symlinking `/cache` files to the local node with a `dd bs=1M` copy
* uses JSON Swif configs
  * faster to create workflows and provides more control than `swif add-job`
  * allows to inspect the workflow before submitting/running it
* CLAS12 workflows
  * proper job exit codes for accurate reporting (and to facilitate automatic retries)
  * file integrity checks, including `hipo-utils -test`, to avoid corrupted input/output files
  * retrieve torus/solenoid scales from RCDB during workflow generation (overridable from command line)
  * automatically retries jobs due to system failures and adjusts job resource reqs if necessary
  * utilize Swif's job tags (e.g. output directory, run/file numbers, coatjava version)
  * periodically write workflow status to clas12mon, for timeline plots and easy global status
    * https://clas12mon.jlab.org/rga/status/decoding/
* includes CLARA log file analysis and slurm job status tools
* initial use was decoding of CLAS12 Spring RGA data, where the requirements included:
  * decode single EVIO files independently (to optimize accessing disordered files on tape)
  * merge sequential HIPO files
  * write merged HIPO files to tape, in sequence (unlike the raw EVIO files)
  * maintain a fixed and available disk space requirement

## TODO
* inlude post-processing tasks (e.g. skim merging, helicity analysis, monitoring)
* write outputs to `/cache` with a `jcache -put` inside the job, pointing downstream dependencies to the `/mss` location

