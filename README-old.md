# clas12-workflow

## Overview
Wrapper tools for [JLab's Swif](https://scicomp.jlab.org/docs/swif2) with full implementation of CLAS12 offline workflows.

## Details
`clas12-workflow.py` is the user interface for workflow generation, and its `-h` option will print the help information below.

All options are not required for all models, but clear feedback is given on missing but required options, and warnings are given on ignored options.  So the _`clas12-workflow.py` command-line can be used to progressively build a proper workflow without needing to know everything upfront_.

Note that the `--config` parameter provides for loading options from a file, and the `--show` option prints the current configuration in a format suitable for the `--config` input.  In all cases, command-line options override the `--config` file.

**_Two key parameters control the overall workflow, `--model` and `--phaseSize`:_**

* **_Model_** describes the different tasks that will be performed (decoding, merging, reconstruction, analysis trains) for all input EVIO/HIPO files.  These tasks are done in a series of jobs that are submitted automatically by Swif after their dependencies are satisfied.
* **_Phase size_** affects how dependencies within the workflow are managed.  Two dependency styles are available: _minimal job-job_ dependencies and _phased_ dependencies.  The former is the default, denoted by a negative `--phaseSize`, and maximizes concurrent batch farm footprint.  The latter groups jobs into N runs per Swif phase (or files if greater than 99), serving to:
  * avoiding huge queues so other priority jobs submitted later can get in
  * promote getting complete runs processed over maximum throughput
  * promote run-ordering on tape over maximum throughput

**_And a couple peculiarities:_**
* For workflows that include decoding, `--decDir` allows to send the decoded files to a different destination than the other tasks (since decoding is unique in that it is generally only done once).
* For workflows that include analysis trains, `--workDir` enables staging the single train outputs in a separate location before merging them by run number (e.g. so temporary files are not written to /cache)

```
ifarm1801> clas12-workflow.py -h

usage: clas12-workflow.py [-h] [--runGroup NAME] [--tag NAME] [--model NAME]
                          [--inputs PATH] [--runs RUN/PATH] [--outDir PATH]
                          [--decDir PATH] [--trainDir PATH] [--workDir PATH]
                          [--logDir PATH] [--coatjava VERSION/PATH]
                          [--clara PATH] [--threads #] [--reconYaml PATH]
                          [--trainYaml PATH] [--phaseSize #] [--mergeSize #]
                          [--trainSize #] [--postproc] [--recharge]
                          [--torus #.#] [--solenoid #.#] [--fileRegex REGEX]
                          [--lowpriority] [--node NAME] [--config PATH]
                          [--defaults] [--show] [--submit] [--version]

Generate a CLAS12 SWIF workflow.

optional arguments:
  -h, --help            show this help message and exit
  --runGroup NAME       (*) run group name
  --tag NAME            (*) e.g. pass1v0, automatically prefixed with runGroup
                        and suffixed by model to define workflow name
  --model NAME          (*) workflow model (dec/decmrg/rec/ana/decrec/decmrgre
                        c/recana/decrecana/decmrgrecana)
  --inputs PATH         (*) name of file containing a list of input files, or
                        a directory to be searched recursively for input
                        files, or a (quoted) shell glob of either. This option
                        is repeatable.
  --runs RUN/PATH       (*) run numbers (e.g. "4013" or "4013,4015" or
                        "3980,4000-4999"), or a file containing a list of run
                        numbers. This option is repeatable.
  --outDir PATH         final data location
  --decDir PATH         overrides outDir for decoding
  --trainDir PATH       overrides outDir for trains
  --workDir PATH        temporary data location for single decoded/train files
                        before merging
  --logDir PATH         log location (otherwise the SLURM default)
  --coatjava VERSION/PATH
                        coatjava version number (or install location)
  --clara PATH          clara install location (unnecessary if coatjava is
                        specified as a VERSION)
  --threads #           number of Clara threads
  --reconYaml PATH      absolute path to recon yaml file (stock options = )
  --trainYaml PATH      absolute path to train yaml file (stock options =
                        trigger/calib)
  --phaseSize #         number of files (or runs if less than 100) per phase,
                        while negative is unphased
  --mergeSize #         number of decoded files per merge
  --trainSize #         number of files per train
  --postproc            enable post-processing of helicity and beam charge
  --recharge            rebuild RUN::scaler (unnecessary if decoding was done
                        with 6.5.6 or later)
  --torus #.#           override RCDB torus scale
  --solenoid #.#        override RCDB solenoid scale
  --fileRegex REGEX     input filename format (for matching run and file
                        numbers)
  --lowpriority         run with non-priority fairshare
  --node NAME           batch farm node type (os/feature)
  --config PATH         load config file (overriden by command line arguments)
  --defaults            print default config file and exit
  --show                print config file and exit
  --submit              submit and run jobs immediately
  --version             show program's version number and exit

(*) = required option for all models, from command-line or config file

```

### Examples

First, setup the environment:

`module load workflow` on a CUE machine (or `source $CLAS12WFLOW/env.[c]sh` for a local install)

Then this would generate a workflow for singles decoding of runs 4013 and 4014, and write its JSON file to the current working directory for inspection:

`clas12-workflow.py --runGroup rga --model dec --runs 4013,4014 --inputs /mss/clas12/rg-a/data --outDir /volatile/clas12/rg-a/test`

You could alternatively specify `--runs 4000-4200` or `--runs filename` to do all runs between 4000 and 4200 or read the runs from a file, respectively.

To also merge the decoded files, change the `--model` option to `decmrg`, to add reconstruction `decmrgrec`, or for just analysis trains `ana`, etc.  The available models are shown with the `-h` option.

After importing the resulting JSON file via `swif import -file filename`, you would `swif run` it to start the workflow.  Alternatively, the `--submit` option would have executed these two Swif steps automatically.

See the [examples](./examples) directory for example command-lines and config files. 

### Monitoring / Control
`swif-status.py` wraps various Swif commands.  By default it just prints all your current workflows' statuses, with command-line options to:
* retry any problem jobs and automatically increase their resource requests if necessary
* push status to clas12mon for timelines
* save the current status and full job details, and publish to a web directory

See [cron/crontab](./cron/crontab) for an example cron job.

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
    * <https://clas12mon.jlab.org/>
  * issues a "jcache put" at the end of each phase to force outputs to tape
* includes CLARA log file analysis and slurm job status tools
* initial use was decoding of CLAS12 Spring RGA data, where the requirements included:
  * decode single EVIO files independently (to optimize accessing disordered files on tape)
  * merge sequential HIPO files
  * write merged HIPO files to tape, in sequence (unlike the raw EVIO files)
  * maintain a fixed and available disk space requirement

