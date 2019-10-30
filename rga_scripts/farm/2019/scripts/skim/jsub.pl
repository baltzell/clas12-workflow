#!/usr/local/bin/perl -w

$limit = $ARGV[0];
$i = 0;
system("ls -1 farmjobs/*.jb > joblist_temp");
open(IN, "joblist_temp");
open(SUB,">>submitted_jobs.txt");
while(<IN>){
    if($i < $limit){
	chomp;
	system("jsub $_");
	#print "submitting $_ \n";

	$i++;
	system("mv $_ farmjobs/\.old_sub");
	#print "moving $_  to farmjobs/\.old_sub \n";
	print SUB $_ . "\n";
    }else{
	last;
    }

}
close(SUB);
close(IN);
system("rm joblist_temp");
