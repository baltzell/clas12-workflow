
$basedir = $ARGV[0];

$pwd = `pwd`;
chomp($pwd);
mkdir $basedir;
mkdir "$basedir/merge";
mkdir "$basedir/skim";
mkdir "$basedir/skim/input";
system("cp scripts/merge/\* $basedir/merge"); 
system("cp scripts/skim/\* $basedir/skim");
system("cd $pwd/$basedir/merge/ \; \./setupJobs");
system("cd $pwd/$basedir/skim/ \; \./setupJobs");
system("cd $pwd");
