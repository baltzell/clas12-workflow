# clas12-workflow

## Overview
Wrapper tools for [JLab's Swif](https://scicomp.jlab.org/docs/swif).

Initially use was decoding of CLAS12 Spring RGA data, where the requirements included:
* Decode single EVIO files, independently (to optimize tape access)
* Merge M sequential HIPO files into 1 HIPO file
* Write merged HIPO files to tape, in sequence (unlike the raw EVIO files)
* Maintain a fixed and available disk space requirement

## Usage

`clas12-workflow -h` will print the help information:

```
  -h, --help          show this help message and exit
  --runGroup NAME     (*) run group name
  --tag NAME          (*) workflow name suffix/tag, e.g. v0, automatically
                      prefixed with runGroup and task to define workflow name
  --model NAME        (*) workflow model set(['decmrgrec', 'decrec', 'ana',
                      'recana', 'decmrgrecana', 'decmrg', 'rec', 'decrecana',
                      'dec'])
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
  --phaseSize #       number of files per phase
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

(*) = required option, from command-line or config file
```

### Generating CLAS12 workflows

First, setup the environment:

`module load workflow`

See `clas12-workflow.py -h` for usage options, and the `$CLAS12WFLOW/examples` directory.  This example would generate a workflow and write its JSON file to the current working directory, for a single decoding workflow for run 4013 and 4014:

`clas12-workflow.py --runGroup rga --model dec --runs 4013,4014 --inputs /mss/clas12/rg-a/data --outDir /volatile/clas12/rg-a/test`

You could alternatively specify `--runs 4000-4200` or `--runs filename` to do all runs between 4000 and 4200 or read the runs from a file.

After importing the resulting file in `./jobs` (via `swif import -file filename`), you would `swif run` it to start the workflow.

* All necessary settings are available from the command line, run it with the `-h` option to see.
* You can also use a configuration file, overridden by additional command line options.
* For merging workflows:
  * N (`phaseSize`) should be significantly larger than M (`mergeSize`) to allow Swif to optimize tape access
  * N should be evenly divisible by M, else you'll always get one smaller merge file per phase and irregular merged file numberings

### Monitoring / Control

`swif-status.py` wraps various Swif commands.  By default just prints all current workflows' statuses, with command-line options to:
* automatically retry any problem jobs, and increase resource requests if necessary
* push status to clas12mon for timelines
* save the current status, accumulating log, and full job details to log files, and publish to web directory

See `$CLAS12WFLOW/cron/swif.cron` for an example cron job, where retry attempts will cause cron to generate an automatic email.

## Details

### Features
* intended to be reusable in the future (see `lib/Swif*` classes)
* automatically overrides Swif/Auger's symlinking `/cache` files to the batch node with a `dd bs=1M` copy.
* uses JSON Swif configs
  * faster to create workflows and provides more control (e.g. job names) than `swif add-job`
* CLAS12 workflows
  * file integrity checks during the jobs, based on return value of `hipo-utils -test`
  * retrieve torus/solenoid scales from RCDB during workflow generation (overridable from command line)
  * utilize Swif's job tags (e.g. output directory, run/file numbers, coatjava version)
  * automatically retries jobs due to system failures and adjusts job resource reqs if necessary
  * periodically write workflow status to clas12mon, for timeline plots and easy global status
    * https://clas12mon.jlab.org/status/decoding/

### TODO
* write outputs to `/cache` with a `jcache -put` inside the job, pointing dependencies to the `/mss` location


