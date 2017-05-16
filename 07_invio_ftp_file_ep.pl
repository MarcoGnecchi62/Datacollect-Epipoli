#!/usr/bin/perl
use strict;
use warnings;
use File::HomeDir;
use File::Basename;
use Net::FTP;
use File::Listing qw(parse_dir);
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );

my $societa = 'SM';

my $scrivania  = File::HomeDir->my_desktop;
my $cartella_remota = "/IN/";
my $cartella_locale = "/Users/italmark/Desktop/periodi/corrente/da_inviare";

my $ftp_url = 'itm-lrp01.italmark.com';
my $ftp_utente = "if";
my $ftp_password = "if";


# Veirifica esistenza cartella locale
#----------------------------------------------------------------------------------------
if (-e $cartella_locale) {

	my @elenco_files;

	# Carico la lista dei file presenti nella cartella di invio e li spedisco
	#--------------------------------------------------------------------------------
	opendir my $DIR, "$cartella_locale";	
	@elenco_files = grep { /\.DAT$/ } grep { !/^\./ } readdir $DIR;
	closedir $DIR;
	
	# Apro la connessione FTP
	#--------------------------------------------------------------------------------
	my $ftp = Net::FTP->new($ftp_url) or
		die "Mancata connessione al sito $ftp_url: $!\n";
	$ftp->login("$ftp_utente","$ftp_password") or 
		die "Login al sito $ftp_url fallito per l'utente $ftp_utente: $!\n";
	$ftp->binary();

	# Invio i file via FTP
	#--------------------------------------------------------------------------------
	my $file_ctl;
	
	$ftp->cwd("$cartella_remota");
	foreach my $file (@elenco_files) {
		$file_ctl = $file;
		$file_ctl =~ s/\.DAT$/\.CTL/ig;
		open my $log_handler, "+>:crlf", "$cartella_locale/$file_ctl" or die $!;
		close ($log_handler);
		
		$ftp->put("$cartella_locale/$file_ctl");
		$ftp->put("$cartella_locale/$file");
	}
	
	# Chiudo la connessione FTP
	#--------------------------------------------------------------------------------
	$ftp->quit();
}
