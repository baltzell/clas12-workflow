#!/usr/bin/perl


$farmworkdir = `pwd`;
chomp($farmworkdir);

@dirs = split( /\//,$farmworkdir);
$cwd = @dirs[scalar(@dirs)-2];

@words = split(/_/,$cwd);
$type = $words[3];
$rungroup = $words[4];
$version = $words[5];
$pass  = "pass1";
$extra = "";
if(scalar @words >5){
        $extra = "_".$words[6];
}

$indir = "/volatile/clas12/trains/$rungroup/$cwd";
$outdir_base = "";
if($type eq "physics"){
    $outdir_base = "/work/clas12/$rungroup/trains/$pass/$version$extra";
}else{
    $outdir_base = "/work/clas12/$rungroup/trains/calibration/$version$extra";
}

@skimnumbers;
@outdirs;

if($type eq "physics"){
        @skimnumbers = ("1","2","3","4","5","6","7","8","9","10","11","12","13");
        @outdirs = ($outdir_base."/skim01_jpsi_tcs",$outdir_base."/skim02_ft_pi0",$outdir_base."/skim03_mesonx_vs",$outdir_base."/skim04_inclusive",$outdir_base."/skim05_inclusiveHadron",$outdir_base."/skim06_positron",$outdir_base."/skim07_epiNCND",$outdir_base."/skim08_ep",$outdir_base."/skim09_ppbar",$outdir_base."/skim10_elec_ft_pip",$outdir_base."/skim11_elect_ft_kaon",$outdir_base."/skim12_elec_3pi",$outdir_base."/skim13_missing_neutron");
}else{
        @skimnumbers = ("6","7","8");
        @outdirs = ($outdir_base."/skim6_ftofhtcc",$outdir_base."/skim7_ctofcnd",$outdir_base."/skim8_ft");
}


@files = `ls $indir`;
%runslist;
foreach $file (@files){
     chomp($file);
     @words = split(/_00/,$file);
     $run = int(substr($words[1],0,4));
     if (! $runslist{$run} && $run > 0) {
        #print($file ." ". $run ."\n");
         $runslist{$run} = 1;
     }
}

@runs = keys %runslist;

foreach $run (@runs){
        #print $run."\n";
}

$nTotal = 0;
$nComplete = 0;

for($nskim = 0; $nskim<(scalar @skimnumbers); $nskim++){
$skimnumber = $skimnumbers[$nskim];
$outdir = $outdirs[$nskim];
$i = 0;
foreach $run (@runs){
    $run_subset = `ls -1 $indir/\*$run\*_$skimnumber\.hipo 2>/dev/null`;
    @run_files = split(/\n/,$run_subset);
    if((scalar @run_files) > 0){
    if(($skimnumber != 6 && $skimnumber != 7) || $type eq "physics"){
       $filename = "$outdir/skim$skimnumber\_$run.hipo";
       $nTotal++;
       if(-e "$filename"){$nComplete++;}else{print "Missing $filename.\n";}
    }elsif($skimnumber==6){
       $filename = "$outdir/skim$skimnumber\_ftof\_$run.hipo";
       $nTotal++;
       if(-e "$filename"){$nComplete++;}else{print "Missing $filename.\n";}
       $filename = "$outdir/skim$skimnumber\_htcc\_$run.hipo";
       $nTotal++;
       if(-e "$filename"){$nComplete++;}else{print "Missing $filename.\n";}
    }elsif($skimnumber==7){
        $filename = "$outdir/skim$skimnumber\_ctof\_$run.hipo";
        $nTotal++;
        if(-e "$filename"){$nComplete++;}else{print "Missing $filename.\n";}
        $filename = "$outdir/skim$skimnumber\_cnd\_$run.hipo";
	$nTotal++;
        if(-e "$filename"){$nComplete++;}else{print "Missing $filename.\n";}
    }	
    }

    $i++;
    }
}
printf("Completed $nComplete out of $nTotal skims - [%4.2f%%]\n",$nComplete*100.0/$nTotal);
