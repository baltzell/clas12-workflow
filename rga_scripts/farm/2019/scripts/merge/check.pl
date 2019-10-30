@files = `ls *.hipo`;

foreach $file (@files){
	chomp($file);
	$exitcode = system("hipo-utils -test $file >>/dev/null 2>/dev/null");
	print $file ." ". $exitcode."\n";
	if($exitcode != 0 ){
		print("bad file! Removing $file \n");
		system("rm -f $file");
	}
}
