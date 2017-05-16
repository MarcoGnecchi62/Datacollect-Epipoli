#!/usr/bin/perl
use strict;
use warnings;
use File::HomeDir;
use File::Copy;
use File::Path;

my $desktop 	= File::HomeDir->my_desktop;

#il paramemtro 0 deve contenere la cartella contenente i file da analizzare
if ( @ARGV == 1 ) {
	if (-e $ARGV[0]) {
		
		# Carico la lista dei file presenti nella cartella
		#-------------------------------------------------------------------------------------------
		my @elenco_files;
		opendir my($DIR), $ARGV[0] or die "Non  stato possibile aprire la directory $ARGV[0]: $!\n";
		@elenco_files = grep { /^DC\d{16}\.DAT$/ } readdir $DIR;
		closedir $DIR;
   
		# Analizzo ognuno dei file
		#-------------------------------------------------------------------------------------------
		foreach my $file (@elenco_files) {
			&sistemazione_numerazione($ARGV[0], $file);
		}
	}
}

sub sistemazione_numerazione {
	my ($path, $file) = @_;
  
	my $temp_file_name = $file;
	$temp_file_name =~ s/^(DC\d{16})\.DAT/$1\.TMP/;
	if (open my $new_file_handler, "+>:crlf", "$path/$temp_file_name" ) {;	
		if (open my $old_file_handler, "<:crlf", "$path/$file") {
			my $line;
			my $line_number = 1;
			my $header = '';
			my $ean_ultima_vendita = '';
			while (!eof($old_file_handler)) {
				$line = <$old_file_handler> ;
				$line =~  s/\n$//ig;
				
				if ($line =~ /^.{20}\s004/) {
					$header = $line;
				}
				
				if ($line !~ /^.{20}\s1\d00/) {
						if ($line =~ /^.{20}\s1\d01(.{13})/) {
								$ean_ultima_vendita = $1;
						}
						if ($ean_ultima_vendita !~ //) {
								if ($line =~ /^(.{20}\s1\d13)\s{13}(.*)$/) {
										$line = $1.$ean_ultima_vendita.$2;
								}
						}
						
						if ($line =~ /^(.{16}(?:3.|04).{5}01.{13})N0001(.*)$/) {
							$line = $1.'N0100'.$2;
						}
						if ($line =~ /^(.{71})990000867(.*)$/) {
							$line = $1.'990000619'.$2;
						}
						if ($line =~ /^(.{71})990000868(.*)$/) {
							$line = $1.'990000677'.$2;
						}
						
						if ($line =~ /^(.{23}90\s{5}29433315.{24}).{18}(.*)$/) {
							$line = $1.'10253    990001120'.$2
						}
						
						
						if ($line =~ /^(\d{8})\d{8}(.{93})$/) {
								print $new_file_handler $1.sprintf('%08d', $line_number).$2."\n";
								$line_number++;
						}	
				}
			}
			close ($old_file_handler);
		}
		close ($new_file_handler);
		
		move("$path/$temp_file_name","$path/$file");
	}
};
