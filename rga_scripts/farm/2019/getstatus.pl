
$pwd = `pwd`;
chomp($pwd);

foreach $arg (@ARGV){
     chomp($arg);
     system("cd $pwd/$arg/skim ; perl status.pl");
     system("cd $pwd/$arg/merge ; perl status.pl");
}
