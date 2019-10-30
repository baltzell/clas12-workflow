
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

@inputFiles = `ls $indir`;
@outputFiles = `ls $outdir`;

$totalInputFiles = scalar @inputFiles;
$totalOutputFiles = 0;
foreach $infile (@inputFiles){
    $hasOutFile = 0;
    chomp($infile);
    for $outfile (@outputFiles){
	chomp($outfile);
        if(index($outfile, $infile) != -1){
            #print "$outfile contains $infile\n";
            $hasOutFile = 1;
        }
    }
    if($hasOutFile == 1){
       $totalOutputFiles++;
    }
}
printf("%4.2f%% of the files have been skimmed Output/Input: $totalOutputFiles/$totalInputFiles\n",$totalOutputFiles*100.0/$totalInputFiles);
