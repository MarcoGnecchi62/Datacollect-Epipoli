#!/usr/bin/perl
use strict;
use warnings;
use File::HomeDir;
use File::Copy;
use List::MoreUtils qw(firstidx);
use Data::Dumper;
use DBI;        # permette di comunicare con il database

# conversione reparti
my @ar_sottoreparti_brix;
my @ar_reparti_brix;
my @ar_codici_brix;
my @ar_codice_sottoreparto_old;
my @ar_codice_sottoreparto_new;

&init_conversione_reparto;

# parametri di configurazione del database
my $database = "cm";
my $hostname = "127.0.0.1";
my $username = "root";
my $password = "mela";

my $dbh;
my $sth;

#il paramemtro 0 deve contenere la cartella contenente i file da analizzare
if ( @ARGV == 1 ) {
	if (-e $ARGV[0]) {

		# Carico la lista dei file presenti nella cartella
		#-------------------------------------------------------------------------------------------
		my @elenco_files;
		opendir my($DIR), $ARGV[0] or die "Non è stato possibile aprire la directory $ARGV[0]: $!\n";
		@elenco_files = grep { /^\d{4}_\d{8}_\d{6}_DC\.TXT$/ } readdir $DIR;
		closedir $DIR;

		&DBConnection;

		# Analizzo ognuno dei file
		#-------------------------------------------------------------------------------------------
		foreach my $file (@elenco_files) {
			&analisi_file($ARGV[0], $file);
		}
	}
}

sub analisi_file {
	my ($path, $file_name) = @_;

	my $line;
	my @transazione = ();

	my $file_name_new = $file_name;
	$file_name_new =~ s/^(.*)\.TXT/$1\.TMP/;

	my $transazione_aperta = 0;
	if (open my $new_file_handler, "+>:crlf", "$path/$file_name_new" ) {;
		if (open my $old_file_handler, "<:crlf", "$path/$file_name") {

			my $negozio = '';
			my $data		= '';
			if ($file_name =~ /^(\d{4})_\d{8}_(\d{6})/) {
				$negozio	= $1;
				$data		= $2;
			}

			while (!eof($old_file_handler)) {
				$line = <$old_file_handler> ;
				$line =~  s/\n$//ig;

				if ($line =~ /^.{31}:H:1.{43}$/) {
					$transazione_aperta = 1;
					@transazione = ();
				};

				if ($transazione_aperta) {
					if ($line !~ /^.{78}$/) {
						$line = sprintf('%-78s', $line);
					}
					if ($line =~ /^(.{31}:F:1.)(?:8|9)(.{41})$/) {
						#print "$line\n";
						$line = $1.'0'.$2;
						#print "$line\n";
					};

					push(@transazione, $line);
				}

				#if ($line =~ /^.{31}:F:1.{34}000000000$/) {
				#	$transazione_aperta = 0;
				#} els

				if ($line =~ /^.{31}:F:1.{43}$/) {
					$transazione_aperta = 0;
					#print "$line\n";

					my $cassa 			= '';
					my $transazione = '';
					if ($line =~ /^\d{4}:(\d{3}):\d{6}:\d{6}:(\d{4})/) {
						$cassa = $1;
						$transazione = $2;
					}

					# eliminazione record X, z, Q
					for (my $i = @transazione-1;$i >= 0; $i-- ) {
						if ($transazione[$i] =~ /^.{31}:(X|z|Q):/) {
							splice @transazione, $i, 1;
						}
					}

					#sistemo :C:19
					for (my $i = @transazione-1;$i >= 0; $i-- ) {
						if ($transazione[$i] =~ /:C:197/) {
							$transazione[$i] =~ s/:C:197/:C:142/ig;
						}
						if ($transazione[$i] =~ /:C:198/) {
							$transazione[$i] =~ s/:C:198/:C:143/ig;
						}
						if ($transazione[$i] =~ /:C:193/) {
							$transazione[$i] =~ s/:C:193/:C:143/ig;
						}
					}

					# sistemo i DCw
					for (my $i = @transazione-1;$i >= 0; $i-- ) {
						if ($transazione[$i] =~ /:D:1/ && $transazione[$i+1] =~ /:C:1/ && $transazione[$i+2] =~ /:w:1/) {
							if ($transazione[$i+1] =~ /C:1.{11}(.{13}).{9}\+(\d{9})/) {
								my $eliminato = 0;

								my $barcode = quotemeta($1);
								my $importo = quotemeta($2);

								my $j = $i;
								while(! $eliminato && $j > 0) {
									if ($transazione[$j]=~ m/:C:1.*$barcode.{9}\-$importo/)  {
										$eliminato = 1;
										$transazione[$j] = '';
									}

									$j--;
								}
								$transazione[$i+1] = '';
							}
						}
					}

					# sistemo le promozioni doppie :G:
					for (my $i=0;$i<@transazione-4;$i++) {
						if ($transazione[$i] =~ /^.{31}(:G:1.*)/) {
							my $riga1 = $1;
							if ($transazione[$i+2] =~ /^.{31}(:G:1.*)/) {
								my $riga2 = $1;
								if($riga1 eq $riga2) {
									$transazione[$i+2] = '';
									$transazione[$i+3] = '';
									if ($transazione[$i] =~ /^(.{31}:G:1.{28})(\d{5})(.*)$/) {
										my $inizio = $1;
										my $punti = $2*2;
										my $fine = $3;

										$transazione[$i] = $inizio.sprintf("%05d",$punti).$fine;
									}
								}
							}
						}
					}

					#sistemo le promozioni sconto 20% 0061
					for (my $i=0;$i<@transazione-1;$i++) {
						if ($transazione[$i] =~ /:m:1.*:0061/ && $transazione[$i+1] =~ /:D:1/) {
							my $swapLine = $transazione[$i];
							$transazione[$i] = $transazione[$i+1];
							$transazione[$i+1] = $swapLine;
						}
					}

					#storno promozioni 0033
					#vengono erroneamente stornate come se fossero 0492
					for (my $i = @transazione-1;$i >= 0; $i-- ) {
						if ($transazione[$i] =~ /:S:17/ && $transazione[$i+2] =~ /:C:1/ && $transazione[$i+3] =~ /:S:17.{10}9980/) {
							my $barcode_S = 'S';
							my $quantita_S = '';
							my $importo_S = '';
							if ($transazione[$i] =~ /:S:17.{10}(.{13})\-(.{8}).(\d{9})/) {
								$barcode_S = quotemeta($1);
								$quantita_S = quotemeta($2);
								$importo_S = quotemeta($3);
							}

							my $barcode_C = 'C';
							my $importo_C = '';
							if ($transazione[$i+2] =~ /:C:1.{11}(.{13})\-\d{8}\+(\d{9})/) {
								$barcode_C = quotemeta($1);
								$importo_C = quotemeta($2);
							}

							if ($barcode_S eq $barcode_C) {
								my $eliminato = 0;
								my $j = $i-1;
								while(! $eliminato && $j > 0) {
									if ($transazione[$j] =~ /:S:1/ && $transazione[$j+2] =~ /:C:1/ ) {
										if ($transazione[$j] =~ /:S:1.{11}$barcode_S\+$quantita_S.$importo_S/) {

											if ($transazione[$j+2] =~ /:C:1.{11}$barcode_C\+.{8}\-$importo_C/) {
												$transazione[$j] = '';
												$transazione[$j+1] = '';
												$transazione[$j+2] = '';

												$transazione[$i] = '';
												$transazione[$i+1] = '';
												$transazione[$i+2] = '';
												$transazione[$i+3] = '';
												$transazione[$i+4] = '';

												$eliminato = 1;
											}
										}
									}
									$j--;
								}
							}
						}
					}


					#storno promozioni 0492
					for (my $i = @transazione-1;$i >= 0; $i-- ) {
						if ($transazione[$i] =~ /:S:17/ && $transazione[$i+2] =~ /:C:1/ && $transazione[$i+3] =~ /:S:17.{10}9980/) {
							my $barcode_S = 'S';
							my $quantita_S = '';
							my $importo_S = '';
							if ($transazione[$i] =~ /:S:17.{10}(.{13})\-(.{8}).(\d{9})/) {
								$barcode_S = quotemeta($1);
								$quantita_S = quotemeta($2);
								$importo_S = quotemeta($3);
							}

							my $barcode_C = 'C';
							my $importo_C = '';
							if ($transazione[$i+2] =~ /:C:1.{11}(.{13})\-\d{8}\+(\d{9})/) {
								$barcode_C = quotemeta($1);
								$importo_C = quotemeta($2);
							}

							if ($barcode_S eq $barcode_C) {
								my $eliminato = 0;
								my $j = $i-1;
								while(! $eliminato && $j > 0) {
									if ($transazione[$j] =~ /:S:1/ && $transazione[$j+4] =~ /:C:1/ ) {
										if ($transazione[$j] =~ /:S:1.{11}$barcode_S\+$quantita_S.$importo_S/) {
											if ($transazione[$j+2] =~ /:S:1.*9980/ && $transazione[$j+4] =~ /:C:1.{11}$barcode_C\+.{8}\-$importo_C/) {
												$transazione[$j] = '';
												$transazione[$j+1] = '';
												$transazione[$j+2] = '';
												$transazione[$j+3] = '';
												$transazione[$j+4] = '';
												$transazione[$j+5] = '';

												$transazione[$i] = '';
												$transazione[$i+1] = '';
												$transazione[$i+2] = '';
												$transazione[$i+3] = '';
												$transazione[$i+4] = '';

												$eliminato = 1;
											}
										}
									}
									$j--;
								}
							}
						}
					}

					#sistemo le promo 0504 anomale
					for (my $i = @transazione-1;$i >= 1; $i-- ) {
						if ($transazione[$i] =~ /:m:.{8}:0504/ && $transazione[$i-1] =~ /:d:/) {
							my $j=$i-1;
							while ($transazione[$j] =~ /:d:/) {
								$transazione[$j] = '';
								$j--;
							}
							$transazione[$i] = '';
						}
					}

					#elimino i resi di vendite che non hanno beneficio
					for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /:S:1(7|8)/ && $transazione[$i+2] =~ /:S:/ && $transazione[$i] !~ /:S:.*9980/ && $transazione[$i+2] !~ /:S:.*9980/) {

							my $trovato = 0;
                            my $search_string = "^.{31}:S:10.{6}:.{3}".substr($transazione[$i], 46, 13)."+".substr($transazione[$i], 60, 8).".".substr($transazione[$i], -9).'$';
							for (my $j = $i-1;$j >= 1; $j-- ) {
								if ($transazione[$j] =~ /$search_string/ && ! $trovato) {
									$transazione[$i] = '';
									$transazione[$i+1] = '';
									$transazione[$j] = '';
									$transazione[$j+1] = '';

									$trovato = 1;
								}
							}

                        }
					}

					#sistemo il codice 998011....
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9982700202001|9982700102004|9981602302000|9981602702008|9981603402006|9981603502003|9981603902001).*$/) ||
							($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9982700302008|9981600102008|9981500502007|9981604202001|9981603302009|9981603602000|9981604302008).*$/) ||
							($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9982700402005|9981600202005|9981500602004|9981602102006|9981603202002|9981603702007|9981604402005).*$/) ||
							($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9982700502002|9981600302002|9981500702001|9981602502004|9981603102005|9981603802004|9981604102004).*$/) ||
							($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9982700602009|9981600402009|9981500802008|9981602602001|9981603002008|9980110005007|9980610002506).*$/) ||
							($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9110110005008|9110110003004|9110110002007|9981602802005|9981602902002|9981604002007|9980110003003|9980110004000).*$/)) {
								if ($transazione[$i] =~ /^(.{46}).{13}(.*$)/) {
									$transazione[$i] = $1.'9980110002006'.$2;
								}
						}
					};


					my @record_G_ACPT = ();
					my @plu_ACPT = ();
					my %reparto_ACPT = ();
					for (my $i = 0;$i < @transazione; $i++) {
						if ($transazione[$i] =~ /:G:1.*P1.*(299.....)/) {
							push @record_G_ACPT, $transazione[$i];
							push @plu_ACPT, $1;

							$transazione[$i] = '';
						}
					}
					if (@plu_ACPT) {
						for (my $i=0; $i < @plu_ACPT; $i++) {
							for (my $j = 0;$j < @transazione; $j++) {
								if ($transazione[$j] =~ /:S:1..:(\d{4}):.{8}$plu_ACPT[$i]/ ) {
									$reparto_ACPT{$plu_ACPT[$i]} = $1;

									$transazione[$j] = '';
									$transazione[$j+1] = ''; #record informativo
								}

								if ($transazione[$j] =~ /:m:1.*:ACPT/ ) {
									$transazione[$j] = '';
								}
							}
						}

						my @temp = ();
						for (my $i=0;$i<@transazione;$i++) {
							if ($transazione[$i] ne '') {
								push @temp, $transazione[$i];
							}
						}
						@transazione = @temp;

						my $posizione_prima_vendita = 0;
						for (my $i=0;$i<@transazione;$i++) {
							if (! $posizione_prima_vendita && $transazione[$i] =~ /:S:1/) {
								$posizione_prima_vendita = $i;
							}
						}

						for (my $i=0; $i < @plu_ACPT; $i++) {
							my $record_S = substr($transazione[0],0,31).':S:101:'.$reparto_ACPT{$plu_ACPT[$i]}.':        '.$plu_ACPT[$i].'+00010010*000000000';
							my $record_i = substr($transazione[0],0,31).':i:100:'.$reparto_ACPT{$plu_ACPT[$i]}.':        '.$plu_ACPT[$i].':000000000033000000';
							my $record_G = $record_G_ACPT[$i];
							my $record_m = substr($transazione[0],0,31).':m:100:  00:0022-10311G1                       ';

							$record_G =~ s/:P1:/:P0:/ig;

							splice @transazione, $posizione_prima_vendita, 0, $record_S, $record_i, $record_G, $record_m;
						}
					}

					#trasformo la promo 0033 in 0493
					for (my $i = @transazione-3 ;$i >= 3; $i-- ) {
						if ($transazione[$i] =~ /:C:142/ && $transazione[$i+1] !~ /:(G|m):/ && $transazione[$i+1] !~ /:S:1.{11}998/ && $transazione[$i-2] !~ /:S:1.{11}998/ ) {
							my $riga_0493 = substr($transazione[$i],0,31).':m:100:  00:0493-10256C3                       ';
							splice @transazione, $i+1, 0, $riga_0493;
						}
					}


					for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:S:1.{2}:(?:\s|\d){4}:\s{3}(9980210002500|9980310002509|9980410002508|9980510002507).*$/) {
							if ($transazione[$i] =~ /^(.{46}).{13}(.*$)/) {
								$transazione[$i] = $1.'9980110009999'.$2;
							}
						}
					};


					for (my $i = @transazione - 3; $i>0; $i-- ) {
						if ($transazione[$i+2] =~ /:m:1.{7}:9927/ && $transazione[$i+1] =~ /:G:131/ && $transazione[$i] =~ /:G:111/ && $transazione[$i-2] =~ /:S:1/) {
							my $transazione_C = '';
							my $barcode = '';
							my $segno = '';
							my $quantita = '';
							if ($transazione[$i-2] =~ /^(.{31}):S:1.{11}(.{13})(\+|\-)(.{4})/) {
								$barcode = $2;
								$segno = $3;
								$quantita = $4;
								if ($quantita eq '0000') {
									$quantita = '0001';
								}
								$transazione_C = $1.':C:142:0001:P0:'.$barcode.$segno.$quantita.'0010-000000000';
							}
							my $transazione_111 = $transazione[$i];
							my $transazione_131 = $transazione[$i+1];

							$transazione[$i] = $transazione_131;
							$transazione[$i+1] = $transazione_111;
							#splice @transazione, $i, 0, $transazione_C;;
						}
					}

					#eliminazione promozione 0493 doppia negozio 3674 - Soresina (fino al 6 luglio 2016)
					for (my $i = 0; $i < @transazione - 3; $i++) {
						if ($transazione[$i] =~ /^(0|3)674/ && $transazione[$i] =~ /^.{9}16070(1|2|3|4|5|6)/ && $transazione[$i] =~ /:S:.*8004030101005/ && $transazione[$i+3] =~ /:m:.*:0493/ ) {
							$transazione[$i] = substr($transazione[$i], 0, 69).'000000299';
							$transazione[$i+2] = '';
							$transazione[$i+3] = '';
						}
					}

					#eliminazione promozione 0493 doppia negozio 3674 - Soresina (fino al 6 luglio 2016)
					for (my $i = 0; $i < @transazione - 3; $i++) {
						if ($transazione[$i] =~ /^.652/ && $transazione[$i] =~ /:i:/ && $transazione[$i+1] =~ /:d:.*29908165/ && $transazione[$i+2] =~ /:m:.*:0504/ ) {
							$transazione[$i+1] = '';
							$transazione[$i+2] = '';
						}
					}

					#trasformazione promo 0504 nella 0055
					for (my $i=0; $i<@transazione;$i++) {
						if ( $transazione[$i] =~ /:m:.*:0504/) {
							$transazione[$i] =~ s/:0504/:0055/ig
						}
					}


					#eliminazione errore posizione 0061
					for (my $i=3;$i<@transazione;$i++) {
						if ($transazione[$i] =~ /:m:.*:0507/) {
							if ($transazione[$i-1] =~ /:m:.*:0061/) {
								if ($transazione[$i-2] =~ /:D:198/) {
									if ($transazione[$i-3] =~ /:D:197/) {
										my $riga_1 = $transazione[$i-3];
										my $riga_2 = $transazione[$i-2];
										my $riga_3 = $transazione[$i-1];
										my $riga_4 = $transazione[$i];

										$transazione[$i-2] = $riga_4;
										$transazione[$i-1] = $riga_2;
										$transazione[$i] = $riga_3;
									}
								}
							}
						}
					}

					#eliminazione sconto consilia nullo
					for (my $i=0;$i<@transazione;$i++) {
						if ($transazione[$i] =~ /^.{31}:m:.{9}0507/ && $transazione[$i-1] !~ /^.{31}:D:/) {
							splice @transazione, $i, 1;
						}
					}

					#trasformazione promozione 507 con vendita fittizia 9999 in catalina
					my $vendita_fittizia_consilia = 0;
					for (my $i=0;$i<@transazione;$i++) {
						if ($transazione[$i] =~ /^.{31}:S:.{12}99990000(1200|3200)/) {
							$vendita_fittizia_consilia += 1;
						}
					}

					if ($vendita_fittizia_consilia) {
						for (my $i=0;$i<@transazione;$i++) {
							if ($transazione[$i] =~ /^(.{31}):m:.{9}0507/) {
								my $header = $1;
								if ($transazione[$i-1] =~ /^(.{31}:D:19)7(.{31})(.{10})/) {
									$transazione[$i] = $header.':w:100:0000:   9999000032009+0001'.$3;
									$transazione[$i-1] = $1.'6'.$2.$3;
								}
							}
						}
					}

					#eliminazione quantita nel record C con articoli a peso
					for (my $i=0;$i<@transazione;$i++) {
						if ($transazione[$i] =~ /^(.{31}:C:.{26}).{4}\....(.{10})$/) {
							$transazione[$i] = $1.'00010010'.$2;
						}
					}

					#aggiungo il record C quando la promozione 0027 è errata
					for (my $i = @transazione - 1; $i > 0; $i--) {
						if ($transazione[$i] =~ /:i:1/ && $transazione[$i+2] =~ /:m:1.*:0027/) {
							if ($transazione[$i+1] =~ /^(.{31}):G:1.{11}(.{13})/) {
								splice @transazione, $i+1, 0, $1.':C:142:0001:P0:'.$2.'+00010010-000000000';
							}
						}
					}

					#0105:002:160509:134945:3612:029:S:101:0001:   7613033430763+00010010*000000199
					#0105:002:160509:135010:3612:063:d:100:0001:P0:7613033430763+00010010*000000199

					#0105:002:160509:123137:3531:003:S:101:0004:   2144640000001+0000.114+000000147
					#0105:002:160509:123217:3531:015:d:100:0004:P0:2144640000001+00010010*000000147

					#0105:002:160509:123217:3531:016:d:100:0004:P0:2144220000001+00010010*000000302
					#0105:002:160509:123217:3531:017:d:100:0004:P0:2143280000006+00010010*000000305
					#0105:002:160509:123217:3531:018:D:196:0000: 0:1:           :00+00000-000000150
					#0105:002:160509:123217:3531:019:w:100:0000:   9872407400155+00010000-000000150

					for (my $i = @transazione - 1; $i > 0; $i--) {
						if ($transazione[$i] =~ /:D:196/ && $transazione[$i-1] !~ /:d:/) {
							my @descrittori = ();
							for (my $k = 1; $k < $i; $k++) {
								if ($transazione[$k] =~ /^(.{31}):S:1\d{2}:0001:.{3}(.*)$/) {
									my $descrittore = $1.':d:100:0001:P0:'.$2;
									if ($descrittore =~ /^(.{60})\d{4}\.\d{3}(.*)$/) {
										$descrittore = $1.'00010010'.$2;
									}
									push @descrittori, $descrittore;
								}

							}
							splice @transazione, $i, 0, @descrittori;
						}
					}


				# spostamento della promozione 0505 se non è vicina alla 0022 corrispondente nrel caso mGdd
				for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:m:.{8}:0505.{31}$/) {
								if ($transazione[$i-2] =~ /^.{31}:d:.{11}:.{13}/) {
										if ($transazione[$i-3] =~ /^.{31}:d:.{11}:(.{13})/) {
												my $ean_0505 = $1;
												$ean_0505 =~ s/\s//ig;

												my $j 		= $i-3;
												my $j_S = 0;
												while ($j>3) {
														if ($transazione[$j] =~ /^.{31}:m:.{8}:0022.{31}$/) {
																if ($transazione[$j-3] =~ /^.{31}:S:.{12}(.{13})/) {
																		my $ean_0022 = $1;
																		$ean_0022 =~ s/\s//ig;
																		if (($j_S == 0) && ($ean_0022 == $ean_0505)){
																			$j_S = $j;
																		}
																}
														}
														$j--;
												}

												if ($j_S != 0) {
														splice @transazione, $j_S+1, 0, splice @transazione, $i-3, 4;
												}
										}
								}
						}
				}

				# spostamento della promozione 0505 se non è vicina alla 0022 corrispondente nrel caso mGd
				for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:m:.{8}:0505.{31}$/) {
								if ($transazione[$i-3] !~ /^.{31}:d:.{11}:.{13}/) {
										if ($transazione[$i-2] =~ /^.{31}:d:.{11}:(.{13})/) {
												my $ean_0505 = $1;
												$ean_0505 =~ s/\s//ig;

												my $j 		= $i-2;
												my $j_S = 0;
												while ($j>3) {
														if ($transazione[$j] =~ /^.{31}:m:.{8}:0022.{31}$/) {
																if ($transazione[$j-3] =~ /^.{31}:S:.{12}(.{13})/) {
																		my $ean_0022 = $1;
																		$ean_0022 =~ s/\s//ig;
																		if (($j_S == 0) && ($ean_0022 == $ean_0505)){
																			$j_S = $j;
																		}
																}
														}
														$j--;
												}

												if ($j_S != 0) {
														splice @transazione, $j_S+1, 0, splice @transazione, $i-2, 3;
												}
										}
								}
						}
				}


					# eliminazione della promozione 0505 con accorpamento sulla 0022 con doppio d
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:m:.{8}:0505.{31}$/) &&
							($transazione[$i-1] =~ /^.{31}:G:.{44}$/) &&
							($transazione[$i-2] =~ /^.{31}:d:.{44}$/) &&
							($transazione[$i-3] =~ /^.{31}:d:.{44}$/) &&
							($transazione[$i-4] =~ /^.{31}:m:.{8}:0022.{31}$/) &&
							($transazione[$i-5] =~ /^.{31}:G:.{44}$/)){

							my $punti_0505;
							if ($transazione[$i-1] =~ /^.{62}(.{6}).{10}$/) {
								$punti_0505 = $1*1;
							}

							my $punti_0022;
							if ($transazione[$i-5] =~ /^.{62}(.{6}).{10}$/) {
								$punti_0022 = $1*1;
							}

							my $punti = $punti_0505 + $punti_0022;
							if ($punti >= 0) {
								if ($transazione[$i-5] =~ /^(.{62}).{6}(.{10})$/) {
									$transazione[$i-5] = $1.'+'.sprintf("%05d",$punti).$2;
								} elsif ($transazione[$i-5] =~ /^(.{62}).{6}(.{10})$/) {
									$transazione[$i-5] = $1.'-'.sprintf("%05d",$punti).$2;
								}
							}
							splice @transazione, $i-3, 4;
						}
					}

					# eliminazione della promozione 0505 con accorpamento sulla 0022 con record d singolo (quantità > 1)
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:m:.{8}:0505.{31}$/) &&
							($transazione[$i-1] =~ /^.{31}:G:.{44}$/) &&
							($transazione[$i-2] =~ /^.{31}:d:.{44}$/) &&
							($transazione[$i-3] =~ /^.{31}:m:.{8}:0022.{31}$/) &&
							($transazione[$i-4] =~ /^.{31}:G:.{44}$/)){

							my $punti_0505;
							if ($transazione[$i-1] =~ /^.{62}(.{6}).{10}$/) {
								$punti_0505 = $1*1;
							}

							my $punti_0022;
							if ($transazione[$i-4] =~ /^.{62}(.{6}).{10}$/) {
								$punti_0022 = $1*1;
							}

							my $punti = $punti_0505 + $punti_0022;
							if ($punti >= 0) {
								if ($transazione[$i-4] =~ /^(.{62}).{6}(.{10})$/) {
									$transazione[$i-4] = $1.'+'.sprintf("%05d",$punti).$2;
								} elsif ($transazione[$i-4] =~ /^(.{62}).{6}(.{10})$/) {
									$transazione[$i-4] = $1.'-'.sprintf("%05d",$punti).$2;
								}
							}
							splice @transazione, $i-2, 3;
						}
					}

					#elimino i descrittori 'd' che si riferiscano ad articoli già utilizzati in un'altra promozione
					# for (my $i = @transazione - 1; $i > 0; $i--) {
# 						my $elimina_descrittore = 0;
# 						if ($transazione[$i] =~ /:d:1.{11}(.{13})/) {
# 							my $barcode_d = $1;
# 							for (my $j = $i -1; $j > 0;$j--) {
# 								if ($transazione[$j] =~ /:C:1.{11}(.{13})/) {
# 									my $barcode_C = $1;
# 									if ($barcode_d eq $barcode_C) {
# 										$elimina_descrittore = 1;
# 									}
# 								}
# 							}
# 							if ($elimina_descrittore) {
# 								splice @transazione, $i, 1;
# 							}
# 						}
# 					}
					#2121940000009
					#individuo i descrittori 'd' multipli e li rendo singoli
					my @ar_descrittori_multipli = ();
					for (my $i = @transazione-1;$i>=0;$i--) {
						if ($transazione[$i] =~ /:d:/) {
							if ($transazione[$i] =~ /^(.{46})(.{13})(.)(\d{4})(.*)$/) {
								my $testata = $1;
								my $barcode = $2;
								if ($barcode !~ /\d{6}000000\d/) {
									my $segno = $3;
									my $molteplicita = $4*1;
									my $piede = $5;
									if ($molteplicita > 0) {
										push @ar_descrittori_multipli, $barcode;
										my $d_singolo = $testata.$barcode.$segno.'0001'.$piede;
										$transazione[$i] = $d_singolo;
										for (my $j=1;$j<=$molteplicita-1;$j++) {
											splice @transazione, $i, 0, $d_singolo;
										}
									}
								}
							}
						}
					}

					#rendo singole anche le vendite degli articoli che compaiono nei descrittori 'd'
					if (@ar_descrittori_multipli > 0) {
						for (my $i = @transazione-1;$i>=0;$i--) {
							if ($transazione[$i] =~ /^(.{31}:S:.{12})(.{13})(.)(\d{4})(.*)$/) {
								my $testata = $1;
								my $barcode = $2;
								my $segno = $3;
								my $molteplicita = $4*1;
								my $piede = $5;
								if ($molteplicita > 1) {
									my $idx = firstidx { $_ eq $barcode } @ar_descrittori_multipli;
									if ($idx >= 0) {
										my $S_singolo = $testata.$barcode.$segno.'0001'.$piede;
										my $i_singolo = $transazione[$i+1];

										$transazione[$i] = $S_singolo;
										for (my $j=1;$j<=$molteplicita-1;$j++) {
											splice @transazione, $i, 0, $i_singolo;
											splice @transazione, $i, 0, $S_singolo;
										}
									}
								}
							}
						}
					}

					#individuo il reso 0022
					for (my $i=@transazione-4;$i>0;$i--) {
						if ($transazione[$i]=~/^.{31}:S:17(.{23})\-(.{18})$/) {
							my $riga_1 = ":S:10$1+$2";
							if ($transazione[$i+2]=~/^.{31}(:G:1.{27})\-(.{5})\-(.{9})$/) {
								my $riga_2 = "$1+$2+$3";
								if ($transazione[$i+3]=~/:m:.*:0022/) {
									my $trovato = 0;
									my $j=$i-1;
									while ($j>0 && ! $trovato) {
										if ($transazione[$j]=~ m/\Q$riga_1/ && $transazione[$j+2]=~ m/\Q$riga_2/) {
											$transazione[$i] = '';
											$transazione[$i+1] = '';
											$transazione[$i+2] = '';
											$transazione[$i+3] = '';

											$transazione[$j] = '';
											$transazione[$j+1] = '';
											$transazione[$j+2] = '';
											$transazione[$j+3] = '';

											$trovato = 1;
										}
										$j--;
									}
								}

							}

						}
					}

					#individuo il reso 0055
					my @reso = ();
					for (my $i = 0; $i < @transazione;$i++) {
						if ($transazione[$i] =~ /(:d:1.{24})\-(.{18})$/) {
							push @reso, $1.'+'.$2;
						}
						if ($transazione[$i] =~ /(:D:1.{27})\-(.{5})\+(.{9})$/) {
							push @reso, $1.'+'.$2.'-'.$3;
						}
						if ($transazione[$i] =~ /(:m:.*0055.*)$/ && @reso > 0) {
							push @reso, $1;

							for (my $j = 0; $j < $i - @reso;$j++) {
								#print "$transazione[$j]\n";
								if ($transazione[$j] ne '' && substr($transazione[$j], 31) eq $reso[0]) {
									my $ok = 1;
									for (my $k = 0; $k < @reso;$k++) {
										#print "$transazione[$j+$k]\n";
										if (substr($transazione[$j + $k], 31) ne $reso[$k]) {
											$ok = 0;
										}
									}

									if ($ok) {
										for (my $k = 0; $k < @reso;$k++) {
											$transazione[$j + $k] = '';
											$transazione[$i - $k] = '';
										}
										last;
									}

								}
							}
							@reso = ();
						}
					}

					#individuo il reso 0492
					my $m0492_presente = 0;
					for (my $i = 0; $i < @transazione;$i++) {
						if ($transazione[$i] =~ /:m:.*:0492/) {
							$m0492_presente = 1;
						}
					}
					if ($m0492_presente) {
						@reso = ();
						for (my $i = 0; $i < @transazione;$i++) {
							if ($transazione[$i] =~ /(:C:1.)2(.{22})(?:\+|\-).{8}\+(.{9})$/) {
								my $prima_riga = $1.'3'.$2.'+00010010-'.$3;
								if ($transazione[$i+1] =~ /:S:17.{10}9.{22}000000000$/) {
									my $seconda_riga = ':m:100:0000:0492';

									push @reso, $prima_riga;
									push @reso, $seconda_riga;

									#eseguo lo storno
									for (my $j = 0; $j < $i - @reso;$j++) {
										#print "$transazione[$j]\n";
										if ($transazione[$j] ne '' && substr($transazione[$j], 31) eq $reso[0]) {
											my $ok = 1;
											for (my $k = 0; $k < @reso;$k++) {
												#print "$transazione[$j+$k]\n";
												if (substr($transazione[$j + $k], 31, length($reso[$k])) ne $reso[$k]) {
													$ok = 0;
												}
											}

											if ($ok) {
												for (my $k = 0; $k < @reso;$k++) {
													$transazione[$j + $k] = '';
													$transazione[$i] = '';
												}
												last;
											}

										}
									}
									@reso = ();
								}
							}
						}
					} else {
						@reso = ();
						for (my $i = 0; $i < @transazione;$i++) {
							if ($transazione[$i] =~ /(:C:1.)2(.{22})(?:\+|\-).{8}\+(.{9})$/) {
								my $prima_riga = $1.'3'.$2.'+00010010-'.$3;
								if ($transazione[$i+1] =~ /:S:17.{10}9.{22}000000000$/) {

									push @reso, $prima_riga;

									#eseguo lo storno
									for (my $j = 0; $j < $i - @reso;$j++) {
										if ($transazione[$j] ne '' && substr($transazione[$j], 31) eq $reso[0]) {
											my $ok = 1;
											for (my $k = 0; $k < @reso;$k++) {
												if (substr($transazione[$j + $k], 31, length($reso[$k])) ne $reso[$k]) {
													$ok = 0;
												}
											}

											if ($ok) {
												for (my $k = 0; $k < @reso;$k++) {
													$transazione[$j + $k] = '';
													$transazione[$i] = '';
												}
												last;
											}

										}
									}
									@reso = ();
								}
							}
						}
					}

					#individuo il reso 0493
					@reso = ();
					for (my $i = 0; $i < @transazione;$i++) {
						if ($transazione[$i] =~ /(:C:1.{24})\-(.{8})\+(.{9})$/) {
							my $prima_riga = $1.'+'.$2.'-'.$3;
							if ($transazione[$i+1] =~ /(:m:1.*:0493.*)$/) {
								my $seconda_riga = $1;

								push @reso, $prima_riga;
								push @reso, $seconda_riga;

								#eseguo lo storno
								for (my $j = 0; $j < $i - @reso;$j++) {
									#print "$transazione[$j]\n";
									if ($transazione[$j] ne '' && substr($transazione[$j], 31) eq $reso[0]) {
										my $ok = 1;
										for (my $k = 0; $k < @reso;$k++) {
											#print "$transazione[$j+$k]\n";
											if (substr($transazione[$j + $k], 31) ne $reso[$k]) {
												$ok = 0;
											}
										}

										if ($ok) {
											for (my $k = 0; $k < @reso;$k++) {
												$transazione[$j + $k] = '';
												$transazione[$i + $k] = '';
											}
											last;
										}

									}
								}
								@reso = ();
							}
						}
					}

					#individuo il reso 0023
					for (my $i = 0; $i < @transazione;$i++) {
						if ($transazione[$i] =~ /(:S:1)(?:7|8)(.{23})\-(.{18})$/) {
							my $prima_riga = $1.'0'.$2.'+'.$3;
							if ($transazione[$i+2] =~ /(:G:1).(.{26})\+(.{5})\-(.{9})$/) {
								my $seconda_riga = $1.'3'.$2.'-'.$3.'+'.$4;

								if ($transazione[$i+3] =~ /:m:1.{7}:0023/) {
									#eseguo lo storno
									for (my $j = 0; $j < $i - @reso;$j++) {
										if ($transazione[$j] ne '' && substr($transazione[$j], 31) eq $prima_riga && substr($transazione[$j+2], 31) eq $seconda_riga && $transazione[$j+3] =~ /:m:1.{7}:0023/ ) {
											$transazione[$i + 2] = '';
											$transazione[$i + 3] = '';

											$transazione[$j + 2] = '';
											$transazione[$j + 3] = '';
										}
									}
								}
							}
						}
					}

					#individuo il reso 0027
					for (my $i = 0; $i < @transazione;$i++) {
						if ($transazione[$i] =~ /(:S:1)(?:7|8)(.{23})\-(.{18})$/) {
							my $prima_riga = $1.'0'.$2.'+'.$3;
							if ($transazione[$i+2] =~ /(:C:142.{22})\-(.{8})\+(.{9})$/) {
								my $seconda_riga = $1.'+'.$2.'-'.$3;
								if ($transazione[$i+3] =~ /(:G:1).(.{26})\+(.{5})\-(.{9})$/) {
									my $terza_riga = $1.'3'.$2.'-'.$3.'+'.$4;
									if ($transazione[$i+4] =~ /:m:1.{7}:0027/) {
										#eseguo lo storno
										for (my $j = 0; $j < $i - @reso;$j++) {
											if ($transazione[$j] ne '' &&
												substr($transazione[$j], 31) eq $prima_riga &&
												substr($transazione[$j+2], 31) eq $seconda_riga &&
												substr($transazione[$j+3], 31) eq $terza_riga &&
												$transazione[$j+4] =~ /:m:1.{7}:0027/ ) {

												$transazione[$i + 2] = '';
												$transazione[$i + 3] = '';
												$transazione[$i + 4] = '';

												$transazione[$j + 2] = '';
												$transazione[$j + 3] = '';
												$transazione[$j + 4] = '';
											}
										}
									}
								}
							}
						}
					}

					#elimino la vendita che viene resa subito
					for (my $i = 2; $i < @transazione;$i++) { # non inizio dalla riga 1 perché la prima vendita non può essere un reso
						if ($transazione[$i] =~ /(:S:1)7(.{23})\-(.{18})$/) {
							my $reso = $1.'0'.$2.'+'.$3;
							if ($transazione[$i-2] ne '' && substr($transazione[$i-2], 31) eq $reso) {#eseguo lo storno
								$transazione[$i+1] = '';
								$transazione[$i] = '';
								$transazione[$i-1] = '';
								$transazione[$i-2] = '';
							}
						}
					}

					#elimino il reso a cui segue subito la stessa vendita
					for (my $i = 2; $i < @transazione;$i++) { # non inizio dalla riga 1 perché la prima vendita non può essere un reso
						if ($transazione[$i] =~ /(:S:1)7(.{23})\-(.{18})$/) {
							my $reso = $1.'0'.$2.'+'.$3;
							if ($transazione[$i+2] ne '' && substr($transazione[$i+2], 31) eq $reso) {#eseguo lo storno
								$transazione[$i] = '';
								$transazione[$i+1] = '';
								$transazione[$i+2] = '';
								$transazione[$i+3] = '';
							}
						}
					}

					#sposto in fondo gli storni
					my @storno = ();
					for (my $i = 0; $i < @transazione;$i++) {
						if ($transazione[$i] =~ /:S:14/) {
							push @storno, $transazione[$i];
							push @storno, $transazione[$i+1];

							$transazione[$i] = '';
							$transazione[$i+1] = '';
						}
					}
					if (@storno > 0) {
						my $ultima_vendita = 0;
						for (my $i = 0; $i < @transazione;$i++) {
							if ($transazione[$i] =~ /:(S|C|m|w):1/ && $transazione[$i] !~ /:m:1.*:0034/){
								$ultima_vendita = $i;
								if ($transazione[$i] =~ /:S:1/) {
									$ultima_vendita++;
								}
							}
						}
						splice @transazione, @transazione-1, 0, @storno;
					}

					#elimino il reso bollone che non abbia anche il reso dello sconto
					for (my $i = @transazione - 1;$i > 0; $i--) {
						if ($transazione[$i] =~ /:S:.7.{10}998011/	&& 	$transazione[$i-1] !~ /:C:/) {
							splice @transazione, $i, 2;
						}
					}

					#carico tutte le promozioni 0492 nell'array @transazione_0492
					my @transazione_0492 = ();
					for (my $j= @transazione - 5;$j>=0;$j--) {
						if ($transazione[$j] =~ /:m:.*:0492/) {$transazione[$j] = ''}

						if ($transazione[$j] =~ /:C:/  && $transazione[$j-2] =~ /:S:.*998011/ && $transazione[$j-4] =~ /:S:/) {

							splice @transazione_0492, 0, 0, $transazione[$j-4], $transazione[$j-3], $transazione[$j-2], $transazione[$j-1], $transazione[$j];

							$transazione[$j-4] = '';
							$transazione[$j-3] = '';
							$transazione[$j-2] = '';
							$transazione[$j-1] = '';
							$transazione[$j] = '';
						}
					}

					my $i = @transazione-1;
 					while ($i>=0) {
 						if ($transazione[$i] eq '') {
 							splice @transazione, $i, 1;
 						} else {
 							$i--;
 						}
 					}

					#isolo per ogni 'd' la vendita corrispondente e calcolo lo sconto ventilato
					$i = @transazione-1;
					while ($i>=0) {

						#determino lo sconto totale
						if ($i-2 > 0 && $transazione[$i] =~ /:m:1.{8}(0055|0507)/ && $transazione[$i-1] =~ /:D:19(6|7):/ && $transazione[$i-2] =~ /:d:/) {
							my $sconto;
							if ($transazione[$i-1] =~ /:D:19(?:6|7):.*(\d{9})$/) {
								$sconto = $1*1;
								if ($transazione[$i-1] =~ /^(.{46})0(.{31})$/) {
									$transazione[$i-1] = $1.'1'.$2;
								}
							}

							#determino la posizione del primo e dell'ultimo descrittore "d"
							my $primo_d = -1;
							my $ultimo_d = $i -2;

							my $j = $ultimo_d;
							while ($j >= 0 && $transazione[$j] =~ /:d:1/) {
								$primo_d = $j;
								$j--;
							}

							#salvo i descrittori 'd' in un array di supporto e leggo i valori su cui ventilare
							my @righe_d_barcode = ();
							my @righe_d_importo_vendita = ();
							my @righe_d_importo_sconto = ();
							for (my $j=$primo_d;$j<=$ultimo_d;$j++) {
								if ($transazione[$j] =~ /:d:.{12}(.{13})(.{5}).{5}(.{9})$/) {
									if ($3*1 != 0) {
										push @righe_d_barcode, $1;
										push @righe_d_importo_vendita, $2*$3;
										push @righe_d_importo_sconto, 0;
									}
								}
							}

							#calcolo ora lo sconto ventilato
							my $totale_valore_vendite_su_cui_ventilare = 0;
							for (my $j=0;$j<@righe_d_barcode;$j++) {
								$totale_valore_vendite_su_cui_ventilare += $righe_d_importo_vendita[$j];
							}
							if($totale_valore_vendite_su_cui_ventilare == 0) {print "$transazione[0]\n"}
							for (my $j=0;$j<@righe_d_barcode;$j++) {
								$righe_d_importo_sconto[$j] = int($sconto*$righe_d_importo_vendita[$j]/$totale_valore_vendite_su_cui_ventilare);
							}
							my $resto = $sconto;
							for (my $j=0;$j<@righe_d_barcode;$j++) {
								$resto -= $righe_d_importo_sconto[$j];
							}
							if ($resto != 0) {
								#determino la vendita massima
								my $valore_max = 0;
								my $riga_max = -1;
								for (my $j=0;$j<@righe_d_barcode;$j++) {
									if ($valore_max < $righe_d_importo_vendita[$j]) {
										$riga_max = $j
									}
								}
								$righe_d_importo_sconto[$riga_max] += $resto;
							}

							#ora taglio la promozione
							for ($j=$primo_d;$j<=$i;$j++) {
								$transazione[$j] = '';
							}

							#cerco le vendite corrispondenti ad ogni 'd', le taglio e compongo lo sconto
							#0492 salvandolo nell'array @transazione_0492
							for (my $j= @righe_d_barcode-1;$j>=0;$j--) {

								my $k = 0;
								my $vendita_trovata = 0;
								while ($k<@transazione && ! $vendita_trovata) {
									if ($transazione[$k] =~ /^(.{31}):S:.{3}:(.{4}):...(.{13})/) {
										if ($3 eq $righe_d_barcode[$j]) {
											$vendita_trovata = 1;

											my $riga_S1 = $transazione[$k];
											my $riga_i1 = $transazione[$k+1];
											my $riga_S2 = $1.':S:101:'.$2.':   9980110009999+00010010*000000000';
											my $riga_i2 = $1.':i:100:0002:   9980110009999:000000000003000000';
											my $riga_C =  $1.':C:143:'.$2.':P0:'.$3.'+00010010'.sprintf('%+010d', $righe_d_importo_sconto[$j]*-1);

											splice @transazione_0492, 0, 0, $riga_S1, $riga_i1, $riga_S2, $riga_i2, $riga_C;

											splice @righe_d_barcode, $j, 1;
											splice @righe_d_importo_vendita, $j, 1;
											splice @righe_d_importo_sconto, $j, 1;

											$transazione[$k] = '';
											$transazione[$k+1] = '';
										}
									}
									$k++;
								}
							}
							if (@righe_d_barcode>0) {
								for (my $j= @righe_d_barcode-1;$j>=0;$j--) {
									my $vendita_trovata = 0;
									my $k = 0;
									while ($k < @transazione_0492 && ! $vendita_trovata) {
										if ($transazione_0492[$k] =~ /:C:.{12}(.{13}).{9}(.{10})$/) {
											if ($1 eq $righe_d_barcode[$j]) {
												$transazione_0492[$k] = substr($transazione_0492[$k],0,68).sprintf('%+010d', $2 - $righe_d_importo_sconto[$j]);
												$vendita_trovata = 1;
											}
										}
										$k++;
									}
								}
							}
						} else {
							$i--;
						}
					}

					#ora aggiungo le transazioni 0492 alle trasazioni principali
					if (@transazione_0492 > 0) {
						splice @transazione, 1, 0, @transazione_0492;
					}

					#elimino le righe vuote dall'array principale
					# $i = @transazione-1;
#  					while ($i>=0) {
#  						if ($transazione[$i] eq '') {
#  							splice @transazione, $i, 1;
#  						} else {
#  							$i--;
#  						}
#  					}
					#print "$transazione[0]\n";
					#eliminazione molteplicità promozioni 0057 e 0493
					for (my $i = @transazione-1;$i>=0;$i--) {
						if ($transazione[$i] =~ /:m:.*:(0057|0493)/) {
							if ($transazione[$i-1] =~ /:C:.{12}(.{13}).(\d{4})....(.{10})$/) {
								my $plu = $1;
								my $molteplicita = $2;
								my $importo = $3;

								my $importo_unitario = int($importo/$molteplicita);
								my $delta_importo = sprintf("%.2f", $importo - $importo_unitario * $molteplicita)*1;
								if ($transazione[$i-3] =~ /:S:.{12}(.{13}).(\d{4})/) {
									if ($plu eq $1 && $molteplicita eq $2 && $molteplicita > 1 && $molteplicita <= 20) {
										my $riga_S = '';
										if ($transazione[$i-3] =~ /^(.{31}:S:.{26})....(.{14})$/) {
											$riga_S = $1.'0001'.$2;
										}
										my $riga_i = $transazione[$i-2];
										my $riga_C = '';
										if ($transazione[$i-1] =~ /^(.{31}:C:.{26})....(.{4})/) {
											$riga_C = $1.'0001'.$2.sprintf('%+010d', $importo_unitario);
										}
										my $riga_m = $transazione[$i];

										$i -= 3;
										splice @transazione, $i,4;
										for (my $j=1 ;$j<=$molteplicita;$j++) {
											splice @transazione, $i, 0, $riga_m;
											if ($delta_importo != 0 && $j==$molteplicita) {
												splice @transazione, $i, 0, substr($riga_C,0,68).sprintf('%+010d', $importo_unitario+$delta_importo);
											} else {
												splice @transazione, $i, 0, $riga_C;
											}
											splice @transazione, $i, 0, $riga_i;
											splice @transazione, $i, 0, $riga_S;
										}
									}
								}
							}
						}
					}

					# sistemazione posizione record k
					my $i_k = -1;
					for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:k:.{44}$/) {
							$i_k = $i;
						}
					}
					if (($i_k != 1) && ($i_k >= 0)) {
						# porto il record k in posizione 2
						splice @transazione, 1, 0, splice @transazione, $i_k, 1;
					}

					# eliminazione attribuzione punti G tipo 6
					my $i_g = -1;
					my $i_m = -1;
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:G:\d6.{42}$/) &&
								($transazione[$i+1] =~ /^.{31}:m:.{8}:0023.{31}$/) &&
								($transazione[$i-2] !~ /^.{31}:S:.{44}$/)) {
							splice @transazione, $i, 2;
						}
					}

					# ripetizione 1
					# eliminazione attribuzione punti G tipo 6
					$i_g = -1;
					$i_m = -1;
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:G:\d6.{42}$/) &&
								($transazione[$i+1] =~ /^.{31}:m:.{8}:0023.{31}$/) &&
								($transazione[$i-2] !~ /^.{31}:S:.{44}$/)) {
							splice @transazione, $i, 2;
						}
					}

					# ripetizione 2
					# eliminazione attribuzione punti G tipo 6
					$i_g = -1;
					$i_m = -1;
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:G:\d6.{42}$/) &&
								($transazione[$i+1] =~ /^.{31}:m:.{8}:0023.{31}$/) &&
								($transazione[$i-2] !~ /^.{31}:S:.{44}$/)) {
							splice @transazione, $i, 2;
						}
					}

					# sistemazione posizione reciproca record m,G transazionali (m<G)
					$i_g = -1;
					$i_m = -1;
					for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:G:\d2.{42}$/) {
							$i_g = $i;
						}
						if ($transazione[$i] =~ /^.{31}:m:.{8}:0034.{31}$/) {
							$i_m = $i;
						}
					}
					if (($i_g >= 0) && ($i_m >= 0)) {
						if ($i_g < $i_m) {
							# porto il record m davanti al record G
							splice @transazione, $i_g, 0, splice @transazione, $i_m, 1;
						}
					}

					for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /:m:/) {
							$transazione[$i] =~ s/0057/0493/ig;
						}
					}

					#elimino i record :Q:
					for (my $i = 0;$i < @transazione-4; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:Q:/) {
							splice @transazione, $i, 1, ('')
						}
					}
					for (my $i=@transazione-1;$i>=0;$i--) {
						if ($transazione[$i] eq '') {
							splice @transazione, $i, 1;
						}
					}

					# sistemazione sequenza bollone SiSiC{X} (C prima di S998011)
					for (my $i = 0;$i < (@transazione-3); $i++ ) {
						if (($transazione[$i] =~ /^.{31}:S:.{44}$/) &&
							($transazione[$i+2] =~ /^.{31}:C:.{44}$/) &&
							($transazione[$i+3] =~ /^.{31}:S:.{12}998011.{26}$/)) {
							splice @transazione, $i+2, 0, splice @transazione, $i+3, 2;
						}
					}

					# sistemazione quantita C storno bollone sequenza SiCSi{X}
					for (my $i = 0;$i < (@transazione-3); $i++ ) {
						if ($transazione[$i+2] =~ /^.{31}:S:\d7.{10}998011.{7}(.{9}).{10}$/) {
							my $quantita_bollone = $1;
							#0179:005:140226:100555:6349:033:S:171:0002:   2264415000002-0000.714-000000927
							if ($transazione[$i] =~ /^.{31}:S:\d7.{23}(.{9}).{10}$/) {
								my $quantita_vendita = $1;
								if ($transazione[$i+4] =~ /^(.{31}:C:.{25})(.{9})(.{10})$/) {
									my $quantita_C = $2;
									if ($quantita_bollone ne $quantita_C) {
										$transazione[$i+4] = $1.$quantita_bollone.$3;
									}
								}
							}
						}
					}

					# sistemazione dei doppi m
					# rimetto in ordine la sequenza C{..C}m{..m}
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:m:.{8}:0493.{31}$/) &&
							($transazione[$i+1] =~ /^.{31}:m:.{8}:0493.{31}$/)) {

							my $i_C = $i-1;
							while (($i_C >= 0) && ($transazione[$i_C] =~ /^.{31}:C:.{44}$/)) {
								$i_C--;
							}
							splice @transazione, $i_C+2, 0, splice @transazione, $i, 1;
						}
					}
					# ...ed ora aggancio ogni coppia Cm alla vendita corrispondente
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:m:.{8}:0493.{31}$/) &&
							($transazione[$i-1] =~ /^.{31}:C:.{44}$/)) {
							my $plu;
							if ($transazione[$i-1] =~ /^.{31}:C:.{11}:(.{13}).{19}$/) {
								$plu = $1;

								my $search_string = "^.{31}:S:.{8}:.{3}$plu.{19}".'$';
								if (($transazione[$i-3] !~ /$search_string/)) {
									my $i_S = 0;
									while (($i_S < @transazione) &&
										   (($transazione[$i_S] !~ /$search_string/) ||
										    (($transazione[$i_S] =~ /$search_string/) && ($transazione[$i_S+2] =~ /^.{31}:C:.{44}$/)))) {
										$i_S++;
									}
									if ($i_S < (@transazione-1)) {
										splice @transazione, $i_S+2, 0, splice @transazione, $i-1, 2;
									}
								}
							}
						}
					}
					#...ed ora aggancio ogni coppia Gm alla vendita corrispondente
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:m:.{8}:0022.{31}$/) &&
							($transazione[$i-1] =~ /^.{31}:G:.{44}$/)) {
							my $plu;
							if ($transazione[$i-1] =~ /^.{31}:G:.{11}:(.{13}).{19}$/) {
								$plu = $1;

								my $search_string = "^.{31}:S:.{8}:.{3}$plu.{19}".'$';
								if (($transazione[$i-3] !~ /$search_string/)) {
									my $i_S = 0;
									while (($i_S < @transazione) &&
										   (($transazione[$i_S] !~ /$search_string/) ||
										    (($transazione[$i_S] =~ /$search_string/) && ($transazione[$i_S+2] =~ /^.{31}:G:.{44}$/)))) {
										$i_S++;
									}
									if ($i_S < (@transazione-1)) {
										splice @transazione, $i_S+2, 0, splice @transazione, $i-1, 2;
									}
								}
							}
						}
					}


					# elimino errore quantita record C = 0 e valore record G = 0 x promozione 0027
					for (my $i = 0;$i < @transazione; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:C:.{44}$/) && ($transazione[$i+1] =~ /^.{31}:G:.{44}$/) &&
								($transazione[$i+2] =~ /^.{31}:m:.{8}:0027.{31}$/)) {

							if ($transazione[$i] =~ /^.{31}:C:.{26}(.{4}).{14}$/) {
								my $quantita_riga_c = $1*1;
								if ($transazione[$i+1] =~ /^.{31}:G:.{34}(.{10})$/) {
									my $valore_riga_g = $1*1;
									if ($quantita_riga_c == 0 and $valore_riga_g == 0) {
										splice @transazione, $i, 3;
									}
								}
							}

						}
					}


					#elimino i bolloni senza record :C:
					for (my $i = 0;$i < @transazione-4; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:S:.[^7]{11}99.*000000000$/ and $transazione[$i+2] !~ /^.{31}:C:1/) {
							splice @transazione, $i, 2, ('','')
						}
					}
					for (my $i=@transazione-1;$i>=0;$i--) {
						if ($transazione[$i] eq '') {
							splice @transazione, $i, 1;
						}
					}

					# elimino gli storni (quando possibile)
					for (my $i = 0;$i < @transazione-1; $i++ ) {
						if (($transazione[$i] =~ /^.{31}:S:\d7.{42}$/) &&
								($transazione[$i] !~ /^.{31}:S:.{12}998011.{26}$/) &&
								($transazione[$i+2] !~ /^.{31}:(C|G|D|d):.{44}$/) &&
								($transazione[$i+2] !~ /^.{31}:S:.{12}998011.{26}$/)) {
							if ($transazione[$i] =~ /^.{46}(.{13})\-(.{18})$/) {
								my $search_string = $1.'\+'.$2;
								$search_string =~ s/\*/\\\*/ig;
								$search_string =~ s/\-/\\\+/ig;

								my $i_S = $i-1;
								while (($i_S >= 0) && ($transazione[$i_S] !~ /$search_string/)) {
									$i_S--;
								}

								if ($i_S > 0) {
									if (($transazione[$i_S+2] !~ /^.{31}:(C|G|D|d):.{44}$/) && ($transazione[$i_S+2] !~ /^.{31}:S:.{12}998011.{26}$/)) {
										#print "$transazione[$i]\n";
										#print "$i, $i_S\n";
										splice @transazione, $i, 2;
										splice @transazione, $i_S, 2;
										$i -= 4;
									}
								}
							}
						}
					}

					#elimino l'errore dell'analyzer quando ho 2 o + vendite dello stesso articolo prima di una promo 0027
					for (my $i = 0;$i < @transazione-4; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:S:.{12}(.{13}).{19}$/) {
							my $ean_1 = $1;
							if ($transazione[$i+4] =~ /^.{31}:m:.{8}:0027.{31}$/) {
								my $i_s = $i-1;
								while (($i_s > 0) &&
											($transazione[$i_s] !~ /^.{31}:C:.{44}$/) &&
											($transazione[$i_s] !~ /^.{31}:G:.{44}$/) &&
											($transazione[$i_s] !~ /^.{31}:m:.{44}$/)) {
									if ($transazione[$i_s] =~ /^.{31}:S:.{12}(.{13}).{19}$/) {
										my $ean_2 = $1;
										if ($ean_1 eq $ean_2) {
											splice @transazione, $i+3, 0, splice @transazione, $i_s, 2;
											$i -= 2;
										}
									}
									$i_s--;
								}
							}
						}
					}

					#elimino l'errore dell'analyzer quando ho 2 o + vendite dello stesso articolo prima di una promo 998011 (0492)
					for (my $i = 0;$i < @transazione-5; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:S:.{12}(.{13}).{19}$/) {
							my $ean_1 = $1;
							if ($transazione[$i+2] =~ /^.{31}:S:.{12}998011.{26}$/) {
								my $i_s = $i-1;
								while (($i_s > 0) &&
											($transazione[$i_s] !~ /^.{31}:C:.{44}$/) &&
											($transazione[$i_s] !~ /^.{31}:G:.{44}$/) &&
											($transazione[$i_s] !~ /^.{31}:m:.{44}$/)) {
									if ($transazione[$i_s] =~ /^.{31}:S:.{12}(.{13}).{19}$/) {
										my $ean_2 = $1;
										if ($ean_1 eq $ean_2) {
											if ($transazione[$i+5] !~ /^.{31}:X:.{44}$/) {
												splice @transazione, $i+3, 0, splice @transazione, $i_s, 2;
											} else {
												splice @transazione, $i+4, 0, splice @transazione, $i_s, 2;
											}
											$i -= 2;
										}
									}
									$i_s--;
								}
							}
						}
					}

					#sistemo i < > e
					for (my $i = 0;$i < @transazione-5; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:C:.{25}(\-|\+).{8}(\<|\>)/) {
							if ($1 eq '+') {
								$transazione[$i] =~ s/\>/\+/ig;
								$transazione[$i] =~ s/\</\-/ig;
							} else {
								$transazione[$i] =~ s/\>/\-/ig;
								$transazione[$i] =~ s/\</\+/ig;
							}
						}
					}

					#e' uno sconto articolo :C:106 che trasformo in bollone
					for (my $i = @transazione-1; $i >= 0; $i-- ) {
						if ($transazione[$i] =~ /^(.{31}):C:106(.{6}).{3}(.*)$/) {
							$transazione[$i] = $1.':C:143'.$2.'P0:'.$3;
							my $record_S = $1.':S:101:0001:   9980110003003+00010010*000000000';
							my $record_i = $1.':i:100:0002:   9980110003003:000000000003000000';
							splice @transazione, $i,0,$record_i;
							splice @transazione, $i,0,$record_S;
						}
					}

					#non interessa se è Department o Plu
					for (my $i = 0;$i < @transazione-5; $i++ ) {
						if ($transazione[$i] =~ /^(.{31}:C:.{9})D(.*)$/) {
							$transazione[$i] = $1.'P'.$2;
						}
					}

					#elimino i doppi bolloni
					for (my $i = 0;$i < @transazione-4; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:S:.[^7]{11}998011/ and $transazione[$i+2] =~ /^.{31}:S:.[^7]{11}998011/) {
							splice @transazione, $i+2, 2, ('','')
						}
					}
					for (my $i=@transazione-1;$i>=0;$i--) {
						if ($transazione[$i] eq '') {
							splice @transazione, $i, 1;
						}
					}

					#elimino i bolloni a reparto
					for (my $i = 0;$i < @transazione-4; $i++ ) {
						if ($transazione[$i] =~ /^.{31}:S:/ and $transazione[$i+2] =~ /^.{31}:S:.{12}998011/ and $transazione[$i+4] =~ /^.{31}:C:/) {
							if ($transazione[$i] =~ /^(.{31}:S:.{12})\s{13}(.*)$/) {
								$transazione[$i] = $1.'     29910007'.$2;
							}
							if ($transazione[$i+4] =~ /^(.{31}:C:.{12})\s{13}(.*)$/) {
								$transazione[$i+4] = $1.'     29910007'.$2;
							}
						}
					}

					# sistemo il trattino nei record m
					for (my $i = 0;$i < @transazione; $i++ ) {
						$transazione[$i] =~ s/(:m:.{13})./$1\-/ig;
					};


					#elimino i doppi C
					$i = 1;
					while ($i<@transazione-2) {
						if ($transazione[$i] =~ /:C:/ and $transazione[$i+1] =~ /:C:/) {
							splice @transazione, $i, 2;
						} else {
							$i++
						}
					}

					#elimino i resi con econvenienza
					$i = 1;
					my $pattern_S;
					my $pattern_C;
					while ($i<@transazione-5) {
						if ($transazione[$i] =~ /:S:17/ and $transazione[$i+2] =~ /:C:142/ and $transazione[$i+3] =~ /:S:17.*000000000$/) {
							if ($transazione[$i] =~ /^.{46}(.{13}).{10}(\d{9})$/) {
								$pattern_S = qr/:S:10.{10}$1.{10}$2/;
							}
							if ($transazione[$i+2] =~ /^.{46}(.{13}).{10}(\d{9})$/) {
								$pattern_C = qr/:C:143.{9}$1.{10}$2/;
							}

							my $j = 0;
							while ($j<@transazione-5) {
								if ($transazione[$j] =~ /$pattern_S/ and $transazione[$j+2] =~ /:S:10.*000000000$/ and $transazione[$j+4] =~ /$pattern_C/) {
									splice @transazione, $i, 5;
									splice @transazione, $j, 5;
									$j = @transazione;
									$i = 0;
								}
								$j++;
							}
						}
						$i++;
					}

					#elimino i resi buono catalina
					$i = 1;
					my $pattern_D;
					while ($i<@transazione-3) {
						if ($transazione[$i] =~ /:D:197.{31}\+(\d{9})/) {
							$pattern_D = qr/^.{31}:D:197.{31}\-$1/;
							my $j = 0;
							while ($j<@transazione-3) {
								if ($transazione[$j] =~ /$pattern_D/ and $transazione[$j+1] =~ /:w:/) {
									splice @transazione, $i, 1;
									splice @transazione, $j, 2;
									$j = @transazione;
									$i = 0;
								}
								$j++;
							}
						}
						$i++;
					}

					#elimino i d raddoppiati in caso di richiamo scontrino
					$i = 1;
					while ($i<@transazione-1) {
						if ($transazione[$i] =~ /:D:/ and $transazione[$i+1] =~ /:d:/) {
							$i++;
							while ($i<@transazione and $transazione[$i] =~ /:d:/) {
								splice @transazione, $i, 1;
							}
						} else {
							$i++
						}
					}

					for (my $i = 1;$i < @transazione; $i++ ) {
						my $tipo_promozione = '0000';
						my $reparto = '00';
						if ($transazione[$i] =~ /^.{9}(\d{2})(\d{2})(\d{2}).{16}:w:(\d).{11}(.{13})/) {
							my $data				= '20'.$1.'-'.$2.'-'.$3;
							my $tipo_transazione 	= $4;
							my $ean					= $5;

							my $reparto					= '0000';
							my $identificativo_catalina = 'CAT'.$ean;

							if ($transazione[$i-1] =~ /^.{31}:D:\d9(5|6)/) {
								if ($transazione[$i-2] =~ /^.{31}:d:...:(\d{4})/) {
									$reparto = $1;
								}
								$tipo_promozione = '0481';
							}
							if ($transazione[$i-1] =~ /^.{31}:D:\d9(7|8)/) {
								$tipo_promozione = '0503';
							}

							$transazione[$i] = sprintf('%-78s', substr($transazione[$i],0,31).':m:'.$tipo_transazione.'01'.':  00:'.$tipo_promozione.'-'.sprintf('%04s',$reparto).$identificativo_catalina);
							if ($tipo_promozione eq '0481' and $transazione[$i-1] =~ /:D:/) {
								$transazione[$i-1] =~ s/^(.{36})6(.{9})1(.*)$/${1}7${2}0$3/;
								# my $j = $i-2;
# 								while ($j > 0 and $transazione[$j] =~ /:d:/) {
# 									$transazione[$j] =~ s/^(.{31}):d:(.*)$/$1:y:$2/;
# 									$j--;
# 								}
							}
						}
					}

					#conversione_reparto
					if ($negozio =~ /^3/) {
						for(my $i = 0; $i < @transazione; $i++) {
							if ($transazione[$i] =~ /^(.{31}:S:...:)(\d{4})(.*)$/) {
								my $testata_linea	= $1;
								my $reparto			= $2;
								my $coda_linea		= $3;

								$reparto = &conversione_reparto($reparto);

								$transazione[$i] = $testata_linea.$reparto.$coda_linea;
							}
						}
					}


					$i = @transazione-1;
					while ($i>=0) {
						if ($transazione[$i] =~ /:m:1.{8}(0481).(\d{4})/) {
							my $reparto = $2;

							#determino la posizione del primo e dell'ultimo descrittore "d"
							my $primo_d = -1;
							my $ultimo_d = $i -2;

							my $j = $ultimo_d;
							while ($j >= 0 && $transazione[$j] =~ /:d:1/) {
								$primo_d = $j;
								$j--;
							}

							my @barcode = ();
							my @reparto = ();
							for (my $k=$primo_d;$k<=$ultimo_d;$k++) {
								if ($transazione[$k] =~ /:d:1..:(\d{4}):...(.{13})/) {
									push @barcode, $2;
									push @reparto, $reparto;
								}
							}

							if (@barcode > 0) {
								for(my $j=0;$j<@barcode;$j++) {
									for(my $k=0;$k<@transazione;$k++) {
										if ($transazione[$k] =~ /:S:/) {
											if (substr($transazione[$k],46,13) eq $barcode[$j]) {
												$transazione[$k]=substr($transazione[$k],0,38).$reparto.substr($transazione[$k],42);
											}
										}
									}
								}
							}
						}
						$i--;
					}

					$i=0;
					while ($i < @transazione) {
						if ($transazione[$i] =~ /^.{31}:S:17.{10}998011/) {
							if ($transazione[$i+2] =~ /^.{31}:X:/  ) {
								if ($transazione[$i+3] =~ /^.{31}:[^C]:/) {
									splice @transazione, $i, 3;
								}
							} elsif ($transazione[$i+2] =~ /^.{31}:[^C]:/) {
								splice @transazione, $i, 2;
							} else {
								$i++;
							}
						} else {
							$i++;
						}
					}

					#sistemazione sconto libero Family
					$i = 0;
					while ($i<@transazione-1) {
						if ($transazione[$i] =~ /:C:1(?:1|3)0/ and $transazione[$i-2] !~ /:S:.{12}9.{22}000000000$/ and $transazione[$i+1] !~ /:D:/ and $transazione[$i+1] !~ /:G:/) {
							if ($transazione[$i] =~ /:C:1(?:1|3)0:.{8}(.{13})\+0001.{5}(\d{9})$/) {
								my $ean_C = $1;
								my $sconto = $2;

								my $j = $i;
								my $eliminato = 0;
								while ($j>0 and ! $eliminato) {
									if ($transazione[$j] =~ /^(.*:S:10.{10})(.{13})(\+0001.{5})(\d{9})$/) {
										my $ean_S = $2;
										my $importo = $4;
										if ($ean_C eq $ean_S and $importo >= $sconto) {
											$importo -= $sconto;
											$transazione[$j] = $1.$2.$3.sprintf('%09d',$importo);
											$eliminato = 1;
											splice @transazione, $i, 1;
										}
									}
									$j--;

									if ($j == 0) { $i++ }
								}
							} else {
								$i++;
							}
						} else {
							$i++
						}
					}

					#sistemazione 0061 sconto dipendente senza beneficio
					$i = 0;
					while ($i<@transazione) {
						if ($transazione[$i] =~ /:m:.{9}0061/) {
							my $beneficio_trovato = 0;
							for (my $j=1;$j<@transazione;$j++) {
								if ($transazione[$j] =~ /:D:198/) {
									$beneficio_trovato = 1;
								}
							}
							if (! $beneficio_trovato) {
								splice @transazione, $i, 1;
							} else {
								$i++;
							}
						} else {
							$i++
						}
					}

					#sistemazione 0061 sconto doppio x 10% aggiuntivo
					for (my $i = 0;$i < @transazione-3; $i++ ) {
						if ($transazione[$i] =~ /:D:198/ and $transazione[$i+1] =~ /:D:198/ and $transazione[$i+2] =~ /:m:.{9}0061/ and $transazione[$i+3] =~ /:m:.{9}0061/) {
							$transazione[$i] =~ /^(.{68})(.*)$/;my $riga_D = $1; my $sconto_1 = $2;
							$transazione[$i+1] =~ /^.{68}(.*)$/;my $sconto_2 = $1;
							$transazione[$i] = $riga_D.sprintf('%+010d', $sconto_1 + $sconto_2);

							splice @transazione, $i+3, 1;
							splice @transazione, $i+1, 1;
						}
					}

					for (my $i = @transazione-1;$i >= 0; $i-- ) {
						if ($transazione[$i] =~ /:m:.*:0492/) {
							splice @transazione, $i, 1;
						}
					}

					#sistemazione reparti bedizzole
					for (my $i = 0; $i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /^046..{28}(S|C|d)/) {
							if ($transazione[$i] =~ /^(.{38})(\d{4})(.{36})$/) {
								my $header  = $1;
								my $reparto = $2;
								my $footer  = $3;

								if ($reparto =~ /000\d/) {
									my $idx = firstidx { $_ eq "$reparto" } @ar_codice_sottoreparto_old;
									if ($idx < 0) {
										$reparto = '0100';
									} else {
										$reparto = $ar_codice_sottoreparto_new[$idx];
									}
									$transazione[$i] = $header.$reparto.$footer;
								}
							}
						}
					}

					# conto il numero di vendite
					my $conteggio_vendite = 0;
					for (my $i = 0;$i < @transazione; $i++ ) {
						if ($transazione[$i] =~ /:S:/) {$conteggio_vendite++}
					}

					# sistemo la numerazione dei record e riscrivo su file la transazione
					if ($conteggio_vendite) {
						my $progressivo = 1;
						for (my $i = 0;$i < @transazione; $i++ ) {
							if ($transazione[$i] =~ /^\d{4}:(\d{3}):\d{6}:(.{11}):\d{3}:(.{46})$/) {
								if ($progressivo == 1000) {$progressivo = 1};
								$transazione[$i] = $negozio.':'.$1.':'.$data.':'.$2.':'.sprintf("%03d",$progressivo).':'.$3;
								$progressivo++;
							}
						};
						foreach (@transazione) {print $new_file_handler "$_\n";};
					}
					@transazione = ();
				};
			}
			close ($old_file_handler);
		}
		close ($new_file_handler);
	}

	move("$path/$file_name_new", "$path/$file_name")
};

sub DBConnection{
	# connessione al database
	$dbh = DBI->connect("DBI:mysql:$database:$hostname", $username, $password);
	if (! $dbh) {
		print "Errore durante la connessione al database, $database!\n";
		return 0;
	}

	$sth = $dbh->prepare(qq{select tipo, reparto from promozioni_buoni where data_inizio <= ? and data_fine >= ?  and ean = ? });
}

sub init_conversione_reparto() {
	# tabella di transcodifica sottoreparti old/new
	push(@ar_codice_sottoreparto_old, '0001');
	push(@ar_codice_sottoreparto_new, '0100');

	push(@ar_codice_sottoreparto_old, '0002');
	push(@ar_codice_sottoreparto_new, '0200');

	push(@ar_codice_sottoreparto_old, '0003');
	push(@ar_codice_sottoreparto_new, '0300');

	push(@ar_codice_sottoreparto_old, '0004');
	push(@ar_codice_sottoreparto_new, '0400');

	push(@ar_codice_sottoreparto_old, '0005');
	push(@ar_codice_sottoreparto_new, '0405');

	push(@ar_codice_sottoreparto_old, '0006');
	push(@ar_codice_sottoreparto_new, '0405');

	push(@ar_codice_sottoreparto_old, '0007');
	push(@ar_codice_sottoreparto_new, '0407');

	push(@ar_codice_sottoreparto_old, '0008');
	push(@ar_codice_sottoreparto_new, '0407');

	push(@ar_codice_sottoreparto_old, '0009');
	push(@ar_codice_sottoreparto_new, '0280');

	push(@ar_codice_sottoreparto_old, '0010');
	push(@ar_codice_sottoreparto_new, '0250');

	push(@ar_codice_sottoreparto_old, '0011');
	push(@ar_codice_sottoreparto_new, '0280');

	push(@ar_codice_sottoreparto_old, '0012');
	push(@ar_codice_sottoreparto_new, '0111');

	push(@ar_codice_sottoreparto_old, '0014');
	push(@ar_codice_sottoreparto_new, '0111');

	push(@ar_codice_sottoreparto_old, '0015');
	push(@ar_codice_sottoreparto_new, '0111');

	#push(@ar_codice_sottoreparto_old, '0016');
	#push(@ar_codice_sottoreparto_new, 'GIFT');

	push(@ar_codice_sottoreparto_old, '0017');
	push(@ar_codice_sottoreparto_new, '0310');

	push(@ar_codice_sottoreparto_old, '0018');
	push(@ar_codice_sottoreparto_new, '0400');

	push(@ar_codice_sottoreparto_old, '0019');
	push(@ar_codice_sottoreparto_new, '0210');

	push(@ar_codice_sottoreparto_old, '0040');
	push(@ar_codice_sottoreparto_new, '0140');

	push(@ar_codice_sottoreparto_old, '0041');
	push(@ar_codice_sottoreparto_new, '0140');

	push(@ar_codice_sottoreparto_old, '0050');
	push(@ar_codice_sottoreparto_new, '0177');

	# tabella di transcodifica reparti/sottoreparti/codici virtuali
	push(@ar_sottoreparti_brix, '0100');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0111');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0112');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0113');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0114');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0120');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0125');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0130');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0140');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0145');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0150');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0155');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0170');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0171');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0175');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0177');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0178');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0180');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0190');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0191');
	push(@ar_reparti_brix, '01');
	push(@ar_codici_brix, '9910000');

	push(@ar_sottoreparti_brix, '0200');
	push(@ar_reparti_brix, '02');
	push(@ar_codici_brix, '9920008');

	push(@ar_sottoreparti_brix, '0210');
	push(@ar_reparti_brix, '02');
	push(@ar_codici_brix, '9920008');

	push(@ar_sottoreparti_brix, '0250');
	push(@ar_reparti_brix, '02');
	push(@ar_codici_brix, '9920008');

	push(@ar_sottoreparti_brix, '0280');
	push(@ar_reparti_brix, '02');
	push(@ar_codici_brix, '9930006');

	push(@ar_sottoreparti_brix, '0300');
	push(@ar_reparti_brix, '03');
	push(@ar_codici_brix, '9940004');

	push(@ar_sottoreparti_brix, '0310');
	push(@ar_reparti_brix, '03');
	push(@ar_codici_brix, '9940004');

	push(@ar_sottoreparti_brix, '0350');
	push(@ar_reparti_brix, '03');
	push(@ar_codici_brix, '9940004');

	push(@ar_sottoreparti_brix, '0380');
	push(@ar_reparti_brix, '03');
	push(@ar_codici_brix, '9940004');

	push(@ar_sottoreparti_brix, '0400');
	push(@ar_reparti_brix, '04');
	push(@ar_codici_brix, '9950001');

	push(@ar_sottoreparti_brix, '0405');
	push(@ar_reparti_brix, '05');
	push(@ar_codici_brix, '9960009');

	push(@ar_sottoreparti_brix, '0407');
	push(@ar_reparti_brix, '07');
	push(@ar_codici_brix, '9980005');

	push(@ar_sottoreparti_brix, '0420');
	push(@ar_reparti_brix, '04');
	push(@ar_codici_brix, '9950001');

	push(@ar_sottoreparti_brix, '0450');
	push(@ar_reparti_brix, '04');
	push(@ar_codici_brix, '9950001');

	#push(@ar_sottoreparti_brix, '2000');
	#push(@ar_reparti_brix, '01');
	#push(@ar_codici_brix, '9910000');
	#
	#push(@ar_sottoreparti_brix, '3000');
	#push(@ar_reparti_brix, '01');
	#push(@ar_codici_brix, '9910000');
	#
	#push(@ar_sottoreparti_brix, '4000');
	#push(@ar_reparti_brix, '01');
	#push(@ar_codici_brix, '9910000');
	#
	#push(@ar_sottoreparti_brix, '5000');
	#push(@ar_reparti_brix, '01');
	#push(@ar_codici_brix, '9910000');
	#
	#push(@ar_sottoreparti_brix, '6000');
	#push(@ar_reparti_brix, '01');
	#push(@ar_codici_brix, '9910000');
}

sub conversione_reparto() {
    my($reparto)=@_;

		# ricodifico i sottoreparti "vecchi" dei negozi Family e Brescia Store
		if ($reparto le '0050') {
			my $idx = firstidx { $_ eq "$reparto" } @ar_codice_sottoreparto_old;
			if ($idx < 0) {
				$reparto = '0100';
			} else {
				$reparto = $ar_codice_sottoreparto_new[$idx];
			}
		}

		# se non trovo il sottoreparto italbrix assegno il sottoreparto di default
		my $idx = firstidx { $_ eq "$reparto" } @ar_sottoreparti_brix;
		if ($idx < 0) {
			$reparto	= '0100';
		}

		return $reparto;
}
