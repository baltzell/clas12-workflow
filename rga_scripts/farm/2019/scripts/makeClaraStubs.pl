#!/usr/local/bin/perl -w

$farmworkdir = `pwd`;
chomp($farmworkdir);
$indir       = "$farmworkdir/input";
@dirs = split( /\//,$farmworkdir);
$cwd = @dirs[scalar(@dirs)-2];
@words = split(/_/,$cwd);
$type = $words[3];
$rungroup = $words[4];
$version = $words[5];

$outdir = "/volatile/clas12/trains/$rungroup/$cwd";
$date        = `date +%F`;
$chunksize = 10;
$jobnumber = 0;

chomp($date);
$runcat = `ls -1 $indir`;
@runlist = split(/\n/,$runcat);

$yaml = "";
if($type eq "physics"){
     $yaml = "/home/rg-a/conductor/yaml/physics/physics_06_19_19.yaml";
}else{
     $yaml = "/home/rg-a/conductor/yaml/calib/train_calib.yaml";
}

push @jobfiles, [ splice @runlist, 0, $chunksize ] while @runlist;

$totalnjobs = scalar @jobfiles;
$njobs = 0;
for ($jobnumber = 0; $jobnumber< scalar @jobfiles; $jobnumber++){
    @currentFileList = @{$jobfiles[$jobnumber]};
    #print scalar @{$jobfiles[$jobnumber]}."\n";
    $filename = sprintf( ">farmjobs/job_%s_%04d\.jb",$date,$jobnumber);
    $track  = ($jobnumber<10)?"debug":"analysis";
    if(! -e "$outdir/skim_$currentFileList[0]_4.hipo"){
    $njobs++;
    open(OUT,$filename) or die("Cannot open file job $filename\n");
    print OUT "PROJECT:          clas12                                           \n";
    print OUT "TRACK:            $track                                      \n";
    print OUT "JOBNAME:          clas12-train-$type-$version-$rungroup-$date-$jobnumber                 \n";
    print OUT "OS:               centos7                                      \n";
    print OUT "COMMAND:          source /group/clas12/packages/setup.csh ; module load coatjava/6b.3.0 ; sh skim.sh $yaml $outdir                             \n";
    print OUT "INPUT_FILES:                                                    \n"; 
    print OUT "                  $farmworkdir/skim.sh                    \n";
    print OUT "                  $farmworkdir/check.pl                    \n";
    foreach $file (@currentFileList){
         print OUT "                  $indir/$file  \n";
    }
    print OUT "CPU: 16 \n";
    print OUT "SINGLE_JOB:       true                                          \n";
    print OUT "MEMORY:           16 GB                                          \n";
    print OUT "DISK_SPACE:       100 GB                                          \n";
    print OUT "TIME:             360                                            \n";
    close(OUT);
    }
}
printf("Generated $njobs job stubs out of $totalnjobs original jobs - Success Rate: [%4.2f%%]\n",($totalnjobs-$njobs)*100.0/$totalnjobs);

if(!(-d $outdir)){
	mkdir $outdir;
}
