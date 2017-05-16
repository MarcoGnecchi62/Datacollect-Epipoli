#!/perl/bin/perl
use strict;     # pragma che dice all'interprete di essere rigido nel controllo della sintassi
use warnings;   # pragma che dice all'interprete di mostrare eventuali warnings
use DBI;        # permette di comunicare con il database
use File::Find; # permette di effettuare cicli sulle directory
use Date::Calc qw(:all);

my $LOG_PATH = "/italmark/log/";
my $OUT_PATH = "/italmark/etl/epipoli/file/";

# parametri di configurazione del database
my $database_ncr	= "ncr";
my $database_cm		= "cm";
my $database_arc	= "archivi";
my $hostname 		= "localhost";
my $username 		= "root";
my $password 		= "mela";

# variabili globali
my $dbh;
my $dbp;
my $dba;
my $sth_0061;
my $sth_0481;
my $sth_multi_target_m;
my $sth_multi_target_d;
my $sth_get_ep_promo_ref;
my $sth_get_ep_promo_2_ref;
my $sth_get_ep_promo_3_ref;
my $sth_get_ep_promo_4_ref;
my $sth_get_ep_promo_5_ref;
my $sth_get_fd_ref;
my $sth_get_codice_articolo;
my $v_quota_sconto_set;
my $v_sconto_0061;
my $v_sconto_0481;
my @negozi; # array di negozi di cui produrre il file per Epipoli
my $line_counter    = 0;    # contatore dei record letti da db
my $tipo_ultima_vendita = 0;
my $ean_ultima_vendita = '';
my $valore_ultima_vendita = 0;
my $codice_articolo_ultima_vendita;
my $reparto_ultima_vendita = 0;
my $articolo_zero = 0;
my $riga_C_virtuale = 0;
my $sconto_transazione = 0;
my $tipo_i = '';
my %movimento;
my %dbrecord;
my $campagna_attuale = 0;
my $campagna_precedente = 0;
my $promo_list;
my $promozione_0505 = 0;
my $categoria_tessera;

my %promozioni;

my $now_string  = localtime;
my $time_rx     = time();

my $log_file_handler;
my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $logdate = sprintf("%04d%02d%02d%02d%02d%02d", $year+1900, $mon+1, $mday,$hour, $min, $sec);
my $log_file_name = $LOG_PATH.$logdate."_DCEpipoli.log";
if (!open $log_file_handler, ">>", $log_file_name) { #open log file in append mode
	print "impossibile aprire il file di log $log_file_name\n $@\n";
	die;
}

if (0) { # per redirigere lo standard output sul file di log
    open OUTPUT, '>>', $log_file_name or die $!;
    STDOUT->fdopen( \*OUTPUT, 'w' ) or die $!;
}

# per gestire l'autoflush del log
my $default_fh = select($log_file_handler);
$| = 1;
select($default_fh);

print "Avvio Elaborazione:      $now_string\n";

# stabilisco la connessione con il db di appoggio per il recupero dei dati
if ( !&DBConnection()) {
    die;
}

# recupero la lista di negozi da elaborare
if (! &LoadHostList()) {
    die;
}

# creo una tabella con tutti e soli i dati di un negozio
foreach my $negozio (@negozi) {
    &CreateHostTable($negozio);
    &CreateEpipoliFile($negozio);
}

my $elab_time   = time() - $time_rx;
$now_string     = localtime;
print "Tempo di elaborazione:   $elab_time secondi\n";
print "Linee analizzate:        $line_counter\n";
print "Fine Elaborazione:       $now_string\n";
close($log_file_handler);

sub DBConnection{
	# connessione al database
	$dbh = DBI->connect("DBI:mysql:$database_ncr:$hostname", $username, $password);
	if (! $dbh) {
		print "Errore durante la connessione al database, $database_ncr!\n";
		return 0;
	}
	
	$sth_multi_target_m = $dbh->prepare(qq{	select id from datacollect_rich where tiporec like binary 'm' and ((body like '0055-%') or (body like '0504-%')) and id > ? order by 1 limit 1});
	
	#cerco i record d e D compresi tra S ed m(multi_target) 
	$sth_multi_target_d = $dbh->prepare(qq{	select tiporec, body from datacollect_rich where tiporec like 'd' and id > ? and id < ?	order by 1});
	
	$dbp = DBI->connect("DBI:mysql:$database_cm:$hostname", $username, $password);
	if (! $dbp) {
		print "Errore durante la connessione al database, $database_cm!\n";
		return 0;
	}
	
	#cerco il codice promozione/campagna di epipoli
	$sth_get_ep_promo_ref = $dbp->prepare(qq{select p.`codice_campagna`, p.`codice_promozione`, p.`classe`
						from promozioni as p, negozi_promozioni as n
						where p.`data_inizio` <= ? and p.`data_fine` >= ? and
						p.`tipo` = ? and p.`codice_articolo` = ? and 
						p.`codice_promozione`=n.`promozione_codice` and n.`negozio_codice`= ?
						order by p.classe});
	
	#cerco il codice promozione/campagna di epipoli
	$sth_get_ep_promo_2_ref = $dbp->prepare(qq{select p.`codice_campagna`, p.`codice_promozione`, p.`classe`
						from promozioni as p, negozi_promozioni as n
						where p.`data_inizio` <= ? and p.`data_fine` >= ? and p.`tipo` = ? and p.`parametro_01` = ? and
						p.`codice_promozione`=n.`promozione_codice` and n.`negozio_codice`= ?
						order by p.`classe`});
	
	#cerco il codice promozione/campagna di epipoli
	$sth_get_ep_promo_3_ref = $dbp->prepare(qq{select p.`codice_campagna`, p.`codice_promozione`, p.`classe`
						from promozioni as p, negozi_promozioni as n
						where p.`data_inizio` <= ? and p.`data_fine` >= ? and p.`tipo` = ? and 
						p.`codice_promozione`=n.`promozione_codice` and n.`negozio_codice`= ?
						order by p.`classe`});
	
	#cerco il codice promozione/campagna di epipoli
	$sth_get_ep_promo_4_ref = $dbp->prepare(qq{select p.`codice_campagna`, p.`codice_promozione`, p.`classe` 
						from promozioni as p, negozi_promozioni as n 
						where p.`data_inizio` <= ? and p.`data_fine` >= ? and p.`tipo` = ? and p.`parametro_02` = ? and 
						p.`codice_promozione`=n.`promozione_codice` and n.`negozio_codice`= ?
						order by p.`classe`});
	
	#cerco il codice promozione/campagna di epipoli
	$sth_get_ep_promo_5_ref = $dbp->prepare(qq{select p.`codice_campagna`, p.`codice_promozione`, p.`classe` 
						from promozioni as p, negozi_promozioni as n 
						where p.`data_inizio` <= ? and p.`data_fine` >= ? and p.`tipo` = ? and p.`codice_reparto` = ? and 
						p.`codice_promozione`=n.`promozione_codice` and n.`negozio_codice`= ?
						order by p.`classe`});

	$dba = DBI->connect("DBI:mysql:$database_arc:$hostname", $username, $password);
	if (! $dba) {
		print "Errore durante la connessione al database, $database_arc!\n";
		return 0;
	}
	
	#cerco il codice articolo
	$sth_get_codice_articolo = $dba->prepare(qq{select `CODCIN-BAR2` from `barartx2` where `BAR13-BAR2` = ? limit 1});
	
  #cerco la corrispondenza di una carta non nimis nella tabella fidelity
	$sth_get_fd_ref = $dba->prepare(qq{select `FDLY-CATEGORIA-CARTA` from fidelity where `FDLY-CODCARTA` = ? and `FDLY-STATO-CARTA` = 'A' order by `FDLY-DATA-VSTATO` DESC});
	
	print "Connessione al db:       OK!\n";
}

sub LoadHostList () {
    my $sth;

    # recupero la lista dei negozi da trattare
    $sth = $dbh->prepare(qq{
        select distinct neg_itm from ncr.negozio order by neg_itm;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    while (my @record = $sth->fetchrow_array()) {
        push(@negozi, $record[0])
    }
    return 1;
}

sub CreateHostTable () {
    my($negozio, @other) = @_;

    print "Creazione DB negozio $negozio: ";

    my $sth;

    # recupero una lista di codici negozi con cui NCR rappresenta i codici italmark
    $sth = $dbh->prepare(qq{
        select distinct neg_ncr from ncr.negozio where neg_itm = ?;
    });

    if (!$sth->execute($negozio)) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    my @list;
    while (my @record = $sth->fetchrow_array()) {
        push(@list, $record[0])
    }
    my $codelist = join(", ",@list);
    #print "$negozio -> $codelist \n";

    $sth = $dbh->prepare(qq{
        drop table if exists ncr.datacollect_negozio;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    $sth = $dbh->prepare(qq{
        create table if not exists ncr.datacollect_negozio like ncr.datacollect_rich;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }


    # $sth = $dbh->prepare(qq{
        # INSERT INTO NCR.DATACOLLECT_NEGOZIO SELECT * FROM NCR.DATACOLLECT_RICH WHERE SOCNEG IN (?) AND TIPO_TRANSAZIONE = 1;
    # });

    my $query = "insert into ncr.datacollect_negozio select * from ncr.datacollect_rich where socneg in ($codelist) and tipo_transazione = 1;";

    #print "$query\n";

    $sth = $dbh->prepare($query);

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    else {
        my $righe = $sth->rows;
        print "OK! ($righe)\n";
    }
}


sub CreateEpipoliFile() {
    my($negozio, @other) = @_;

    # per un negozio possono essere presenti più giorni di datacollect
    # recupero le diverse date di datacollect
    my $sth_datadc =    $dbh->prepare(qq{
                            select distinct datadc from datacollect_negozio
                        });

    # seleziono le date
    if (!$sth_datadc->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    my $datadc;
    while (my @date = $sth_datadc->fetchrow_array()) {
        $datadc = $date[0];
		
		if (! &loadPromozioniAttive($datadc, $negozio)) {
			die;
		}

        # statement di selezione del record
        my $sth_select_record =    $dbh->prepare(qq{
            select      id                ,
                        socneg            ,
                        numcassa          ,
                        data              ,
                        ora               ,
                        transazione       ,
                        riga              ,
                        tiporec           ,
                        code1             ,
                        code2             ,
                        code3             ,
                        code4             ,
                        body              ,
                        ifnull(tipo_transazione , ""),
                        ifnull(negozio          , ""),
                        ifnull(articolo         , ""),
                        ifnull(tessera          , ""),
                        ifnull(valore_netto     , ""),
                        ifnull(tot_scontrino    , ""),
                        ifnull(tot_reparto      , ""),
                        ifnull(sconto_transaz   , ""),
                        ifnull(punti_transaz    , ""),
                        ifnull(punti_reparto    , ""),
                        ifnull(tipo_i           , ""),
                        ifnull(quota_sc_tran    , 0),
                        ifnull(barcode_itm      , 0),
						ifnull(promolist        , 0),
						ifnull(quota_sc_tran_0061, 0),
						ifnull(quota_sc_rep_0481, 0)
            from        ncr.datacollect_negozio
            where       datadc = ?
            });

        # seleziono i record dalla tabella datacollect
        if (!$sth_select_record->execute($datadc)) {
            print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
            return 0;
        }

        my $righe = $sth_select_record->rows;

        if ($righe) {
            # il file per Epipoli Ë creato solo se ho trovato qualcosa per il negozio in questione
            my $file_handler;
            my $file_name   = $OUT_PATH."DC".$datadc."0".$negozio."001.DAT";

			&GetCodiciCampagna($datadc);

            if (!open $file_handler, ">", $file_name) { #open log file in append mode
                print "Impossibile aprire il file $file_name\n";
                die;
            }

            # scorro il result set
            my $line_counter    = 0;
			my $test_data = 1;
            while (my @riga = $sth_select_record->fetchrow_array()) {
                if (! &RecordManager($datadc, $file_handler, \$line_counter, $negozio, @riga)) {
                    $test_data = 0;
				}
            }

            close $file_handler;
			if (! $test_data) {
				rename $file_name, $OUT_PATH."ERRORE_DC".$datadc."0".$negozio."001.DAT";
			}
        }
    }
}

sub RecordManager () {
	my($datadc, $file_handler, $ref_line_counter, $pdv, @riga) = @_;
	my $test_data = 1;
	my $filler;
	undef(%dbrecord);
	$dbrecord{'id'}					= $riga[0];
	$dbrecord{'socneg'}				= $riga[1];
	$dbrecord{'numcassa'}			= $riga[2];
	$dbrecord{'data'}				= $riga[3];
	$dbrecord{'ora'}				= $riga[4];
	$dbrecord{'transazione'}		= $riga[5];
	$dbrecord{'riga'}				= $riga[6];
	$dbrecord{'tiporec'}			= $riga[7];
	$dbrecord{'code1'}				= $riga[8];
	$dbrecord{'code2'}				= $riga[9];
	$dbrecord{'code3'}				= $riga[10];
	$dbrecord{'code4'}				= $riga[11];
	$dbrecord{'body'}				= $riga[12];
	$dbrecord{'tipo_transazione'}	= $riga[13];
	$dbrecord{'negozio'}			= $riga[14];
	$dbrecord{'articolo'}			= $riga[15];
	$dbrecord{'tessera'}			= $riga[16];
	$dbrecord{'valore_netto'}		= $riga[17];
	$dbrecord{'tot_scontrino'}		= $riga[18];
	$dbrecord{'tot_reparto'}		= $riga[19];
	$dbrecord{'sconto_transaz'}		= $riga[20];
	$dbrecord{'punti_transaz'}		= $riga[21];
	$dbrecord{'punti_reparto'}		= $riga[22];
	$dbrecord{'tipo_i'}				= $riga[23];
	$dbrecord{'quota_sc_tran'}		= $riga[24];
	$dbrecord{'barcode_itm'}		= $riga[25];
	$dbrecord{'promolist'}			= $riga[26];
	$dbrecord{'quota_sc_tran_0061'}	= $riga[27];
	$dbrecord{'quota_sc_rep_0481'}	= $riga[28];

	# elimino le tessere non Nimis
	$categoria_tessera = 6; #sconto enti vari
	if (($dbrecord{'tessera'} ne '') && ($dbrecord{'tessera'} !~ /^046\d{10}$/)) {
			if ($sth_get_fd_ref->execute($dbrecord{'tessera'})) {
				while (my @record = $sth_get_fd_ref->fetchrow_array()) {
					$categoria_tessera 	= $record[0];
					if ($categoria_tessera == 0) {
						$categoria_tessera = 6;#sconto enti vari
					}
					
				}
			}
		$dbrecord{'tessera'} = '';
	}
	
	# inizio a costruire il record di datacollect per Epipoli
	my $data_riferimento = $datadc;
	
	#quota sconto nel caso di promozione D0055
	$v_quota_sconto_set	= 0;

	# identificativo tipo record
	# vale 0 per i record i testata, 1 per i record di movimento
	my $id_tipo_record = 1;

	my $dettaglio = "";# parte variabile a seconda del tipo di record
	
	my $skip =0;

	if ($dbrecord{'tiporec'} =~ /H/) {
		$id_tipo_record          	= 0;
		$tipo_ultima_vendita     	= 0;
		$reparto_ultima_vendita  	= 0;
		my $tipo_transazione     	= "04";
		my $numero_transazione   	= $dbrecord{'transazione'};
		my $numero_cassa         	= $dbrecord{'numcassa'};
		my $numero_cassiere      	= substr($dbrecord{'body'},0,16);
		my $data                 	= "20".$dbrecord{'data'};

		if(! &TestData($pdv, $data, $datadc, $numero_transazione, $numero_cassa)) {
			$test_data = 0;
		} else {
			$test_data = 1;
		}

		my $ora                  = substr($dbrecord{'ora'},0,4);
		my $carta                = $dbrecord{'tessera'};
		my $tipo_acquisizione    = 0; # non è possibile sapere se la tessera è stata letta o digitata
		
		$v_sconto_0061 = 0;

		$numero_cassiere =~ s/\s//ig; # rimozione degli spazi
		$filler = "                                             ";
		$dettaglio = sprintf("%02d%04d%04d%06s%08d%04d%13s%01d",$tipo_transazione,$numero_transazione,$numero_cassa,$numero_cassiere,$data,$ora,$carta,$tipo_acquisizione).$filler;
	} else {
	    $filler = "   ";
		&NewMovimento();

		if ($dbrecord{'tiporec'} =~ /S/) {
			&S_Record();
			if($movimento{'codice_ean'} ge '9980110000000' and $movimento{'codice_ean'} le '9980110009999') {
				$skip = 1;# se è un bollone non lo scrive
			} else {
				$promo_list = $dbrecord{'promolist'};
			}
		} elsif ($dbrecord{'tiporec'} =~ /F/) {
			&F_Record();
		} elsif ($dbrecord{'tiporec'} =~ /m/) {
			&m_Record();
			$skip = 1;
		} elsif ($dbrecord{'tiporec'} =~ /C/) {
			&C_Record();
		} elsif ($dbrecord{'tiporec'} =~ /c/) {
			&C_Record();
		} elsif ($dbrecord{'tiporec'} =~ /G/) {
			&G_Record();
		} elsif ($dbrecord{'tiporec'} =~ /D/) {
			#&D_Record();
			$skip = 1;
		} elsif ($dbrecord{'tiporec'} =~ /d/) {
			$skip = 1;
			if ($dbrecord{body} =~ /.{3}(.{13})(.{5})(.)(.{3})(.)(.{9})/) {
				my $ean				= $1;
				my $quantita		= $2;
				my $decimale		= $3;
				my $cifre_decimali	= $4;
				my $segno			= $5;
				my $importo			= $6;
				
				$ean =~ s/\s//ig;
				
				if ($decimale eq '.') {# quantit‡ decimale
					$quantita 	= $quantita.$decimale.$cifre_decimali;
					$quantita 	=~ s/-//ig;
				}
				
				if ($segno eq '*') {
					$importo = "+".$importo
				} else {
					$importo = $segno.$importo
				}
			}
		} elsif ($dbrecord{'tiporec'} =~ /(i|V|t|T|k|e|B|b|j)/) {
			$skip = 1;
		} else {
			$skip = 1;
			print $log_file_handler "Skip record $dbrecord{'tiporec'}\n";
		}

		$dettaglio =sprintf("%01d%02d%13s%1s%4s%09d%01d%09d%-9s%-9s%02d%-10s%13s%01d",
							$movimento{'tipo_operazione'},
							$movimento{'movimento'},
							$movimento{'codice_ean'},
							$movimento{'flag_reparto'},
							$movimento{'reparto_cassa'},
							$movimento{'valore_operazione'},
							$movimento{'unita_misura'},
							$movimento{'quantita'},
							$movimento{'codice_campagna'},
							$movimento{'codice_promozione'},
							$movimento{'numero_set'},
							$movimento{'codice_prodotto'},
							$movimento{'carta'},
							$movimento{'tipo_acquisizione'}).$filler;
	}

    if (!$skip) {
		my $numero_sequenza;
		my $record;
		my $mov;
			
		if ($dbrecord{'tiporec'} =~ /F/ and $v_sconto_0061 != 0) {
			$articolo_zero = 1;
		}
		
		if ($articolo_zero) { # se il movimento è relativo ad un beneficio transazionale occorre introdurre una vendita fittizia dell'articolo zero
			$$ref_line_counter++;
			$mov = '';
			&ArticoloZero(\$mov);
			$numero_sequenza = $$ref_line_counter;
			$record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, 1, $mov).$filler;
			print $file_handler "$record\n";
			#print $log_file_handler "[NCR] $dbrecord{'id'} -> $numero_sequenza [EP]\n";
		}
		
		if ($dbrecord{'tiporec'} =~ /F/ and $v_sconto_0061 != 0) {
			$$ref_line_counter++;
			$mov = '';
			&Sconto_0061(\$mov, $v_sconto_0061);
			$numero_sequenza = $$ref_line_counter;
			$record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, 1, $mov).$filler;
			print $file_handler "$record\n";
		}
	
		# ora scrivo il record del movimento analizzato
		$$ref_line_counter++;
		$numero_sequenza = $$ref_line_counter;
		$record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, $id_tipo_record, $dettaglio);
		#ripulisco il movimento 77 nel caso abbia barcode e quantita
		if ($record =~ /^(.{23})77.{18}(.*)$/) {
			$record = $1.'77'.sprintf('%18s','').$2;
		}
		
		#Se è un dona con nimis aggiungo il record C con valore 0
		if ($riga_C_virtuale) {
			$riga_C_virtuale = 0;
			
			$$ref_line_counter++;
			$numero_sequenza = $$ref_line_counter;
			my $record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, $id_tipo_record, $dettaglio);
			if ($record =~ /^(.{23})\d\d(.{18})\d{9}(.*)$/ ) {
				my $record = $1.'94'.$2.'000000000'.$3;
				print $file_handler "$record\n";
			}
		}
		print $file_handler "$record\n";
		
		#print $log_file_handler "[NCR] $dbrecord{'id'} -> $numero_sequenza [EP]\n";
		
		if ($dbrecord{'quota_sc_rep_0481'} != 0) {
			$$ref_line_counter++;
			$mov = '';
			&Sconto_0481(\$mov, $dbrecord{'quota_sc_rep_0481'}, $dbrecord{'code4'});
			$numero_sequenza = $$ref_line_counter;
			$record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, 1, $mov).$filler;
			print $file_handler "$record\n";
		}
		
		if ($v_quota_sconto_set) {
			$$ref_line_counter++;
			$mov = '';
			&ScontoSet(\$mov);
			$numero_sequenza = $$ref_line_counter;
			$record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, 1, $mov).$filler;
			print $file_handler "$record\n";
		}
		
		if ($sconto_transazione) { # se il movimento (di vendita) prevede uno sconto transazionale bisogna scrivere un movimento di beneficio
			$$ref_line_counter++;
			$mov = '';
			&ScontoTransazionale(\$mov);
			$numero_sequenza = $$ref_line_counter;
			$record = sprintf("%08d%08d%-5s%01d%s", $data_riferimento, $numero_sequenza, $pdv, 1, $mov).$filler;		
			print $file_handler "$record\n";
			#print $log_file_handler "[NCR] $dbrecord{'id'} -> $numero_sequenza [EP]\n";
			#print $log_file_handler "ScontoTransazionale: $numero_sequenza\n";
		}
	}
	return $test_data;
}

sub NewMovimento() {
	undef(%movimento);
	$movimento{'tipo_operazione'} 		= 0;
	$movimento{'movimento'}				= 0;
	$movimento{'codice_ean'}			= "";
	$movimento{'flag_reparto'}			= "";
	$movimento{'reparto_cassa'}			= "";
	$movimento{'valore_operazione'}		= 0;
	$movimento{'unita_misura'}			= 0;
	$movimento{'quantita'}				= 0;
	$movimento{'codice_campagna'}		= "";
	$movimento{'codice_promozione'}		= "";
	$movimento{'numero_set'}			= 0;
	$movimento{'codice_prodotto'}		= "";
	$movimento{'carta'}					= "";
	$movimento{'tipo_acquisizione'}		= 0;
}

sub S_Record() {
	# da utilizzare per interpretare i record sottostanti (c,p..)
	$tipo_ultima_vendita 		= $dbrecord{'code2'};
	$reparto_ultima_vendita 	= $dbrecord{'code4'};
	$tipo_i 			= $dbrecord{'tipo_i'};
	#$promo_list 			= $dbrecord{'promolist'};

	my $flag_farmaceutico   ;
	my $codice_articolo     ;
	my $qta                 ;
	my $decimale            ;
	my $decimali_pxc        ;
	my $segno               ;
	my $valore              ;
	if ($dbrecord{'body'}   =~ /^((A|\d|\s){16})((\+|\-|\d){5})((0|\.){1})(\d{3})((\*|\+|\-|\d){1})(\d{9})$/) {
		$codice_articolo    = $1;
		$qta                = $3;
		$decimale           = $5;
		$decimali_pxc       = $7;
		$segno              = $9;
		$valore             = $10;
		if ($segno eq '*') {$valore = "+".$valore} else {$valore = $segno.$valore};
	} else {
		print "Record S anomalo: $dbrecord{'body'} \n";
		return 0;
	}

	# codice_articolo
	$codice_articolo =~ s/\s//ig; # rimozione di tutti gli spazi
	if ($codice_articolo eq "") { # vendita a reparto
		$movimento{'flag_reparto'}	= "Y";
		$movimento{'codice_ean'}	= "";
		$movimento{'codice_prodotto'}	= "";
	} else {
		$movimento{'flag_reparto'}	= "N";
		$movimento{'codice_ean'}	= $codice_articolo;         # questo è quello letto dallo scanner
		$movimento{'codice_ean'}	= $dbrecord{'barcode_itm'}; # questo è quello di anagrafica
		$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
	}

	# il contenuto del campo $decimale è '.' oppure 0
	if($decimale eq '.') {# quantit‡ decimale
		$qta 	= $qta.$decimale.$decimali_pxc;
		$qta 	=~ s/-//ig;
		$qta 	= 1000*$qta;
		$movimento{'unita_misura'}	= 1;# grammi
	} else {# se il campo $decimale vale 0, il campo $decimali_pxc vale 010
		$qta = 1000*$qta;
		$qta =~ s/-//ig;
		$movimento{'unita_misura'}		= 0;# cad
	}

	# vendita
	if($dbrecord{'code2'} =~ /0/) {
		$movimento{'tipo_operazione'} 		= 0;
		$movimento{'movimento'}			= 01;
		if ($valore =~ /-/) {
			$movimento{'tipo_operazione'} 	= 1;
			$movimento{'movimento'}		= 01;
		}
	} elsif ($dbrecord{'code2'} =~ /1/) {# vendita negativa
		$articolo_zero = 1; # la vendita negativa è considerata uno sconto generico di tipo D preceduto dalla vendita dell'articolo zero
		$movimento{'tipo_operazione'} 		= 1;
		$movimento{'movimento'}			= 11;
		if ($valore =~ /\+/) {
			$movimento{'tipo_operazione'} 	= 0;
			$movimento{'movimento'}		= 11;
		}
	} elsif ($dbrecord{'code2'} =~ /2/) {# vendita negativa
		$articolo_zero = 1; # la vendita negativa è considerata uno sconto generico di tipo D preceduto dalla vendita dell'articolo zero
		$movimento{'tipo_operazione'} 		= 0;
		$movimento{'movimento'}			= 11;
		if ($valore =~ /\+/) {
			$movimento{'tipo_operazione'} 	= 1;
			$movimento{'movimento'}		= 11;
		}
	} elsif (  $dbrecord{'code2'} =~ /4/) {# reso
		$movimento{'tipo_operazione'} 		= 1;
		$movimento{'movimento'}			= 01;
	} elsif (  $dbrecord{'code2'} =~ /7/) {# storno
		$movimento{'tipo_operazione'} 		= 1;
		$movimento{'movimento'}			= 01;
	} elsif ($dbrecord{'code2'} =~ /8/) {# annullo
		$movimento{'tipo_operazione'} 		= 1;
		$movimento{'movimento'}			= 01;
	}

	$valore =~ s/-//ig;
	$valore = 1*$valore;
	if ($movimento{'unita_misura'} == 0) {
		$valore =  $valore*$qta/1000;
	}
	
	$movimento{'reparto_cassa'}	= $dbrecord{'code4'};
	$movimento{'valore_operazione'}	= $valore;
	$movimento{'quantita'}		= $qta;
	$movimento{'codice_campagna'}	= "";   #ok
	$movimento{'codice_promozione'}	= "";   #TODO
	$movimento{'numero_set'}	= 0;
	$movimento{'carta'}		= "";
	$movimento{'tipo_acquisizione'}	= 0;
	
	$ean_ultima_vendita 			= $movimento{'codice_ean'};
	$codice_articolo_ultima_vendita = $movimento{'codice_prodotto'};
	if ($movimento{'codice_ean'} !~ /^998011/) {
		$valore_ultima_vendita			= $movimento{'valore_operazione'};
	}

	# se c'è uno sconto transazionale e quindi una quota per la vendita occorre inserire il beneficio di ripartizione
	if ($dbrecord{'quota_sc_tran'}*1 ne 0) {
		#print $log_file_handler "Devo scrivere un record di beneficio di $dbrecord{'quota_sc_tran'} \n";
		 $sconto_transazione = 1; #momentaneamnete eliminato per la nuova promo 0061 (MG 5/7/14)
	}
	
	if ($dbrecord{'quota_sc_tran_0061'} != 0) {
		$v_sconto_0061 += $dbrecord{'quota_sc_tran_0061'}
	}
	
	$v_quota_sconto_set = 0;
	#se c'è uno sconto set (0055) devo calcolarlo e determinare la quota relativa a questa vendita
	if (($dbrecord{'promolist'} =~ /_D0055/) || ($dbrecord{'promolist'} =~ /_D0504/)) {
		#determino la posizione del record m che ha causato l'inserimento del tipo 0055 in promolist
		#tutti i record 'd' e 'D' sono compresi tra questa vendita ed il record 'm'		
		 if ($sth_multi_target_m->execute($dbrecord{'id'})) {
			my $id_end = 0;
			while (my @riga = $sth_multi_target_m->fetchrow_array()) {
				$id_end =  $riga[0];
			}
			
			 if ($sth_multi_target_d->execute($dbrecord{'id'}, $id_end)) {
				my @ar_articolo		= ();
				my @ar_quantita		= ();
				my @ar_importo		= ();
				my @ar_sconto		= ();
				my $id_record_corrente	= 0;
				my $id_record_maggiore	= 0;
				my $v_totale_importo	= 0;
				my $v_totale_sconto	= 0;
				my $v_resto_sconto	= 0;
				while (my @riga = $sth_multi_target_d->fetchrow_array()) {
					if ($riga[0] =~ /d/) {
						if ($riga[1] =~ /^(?:P|D)(?:1|0):((?:\s|\d){13})((?:\+|\-|\d){5})(\d|\.|\*)(\d{3})(\+|\-|\*)(\d{9})$/) {
							push(@ar_articolo, $1);
							push(@ar_quantita, $2*1);
							push(@ar_importo,  $6*1);
							$v_totale_importo += $ar_importo[$#ar_importo];
							$ar_articolo[$#ar_articolo] =~ s/^\s+//;
						}
					} 
					if ($riga[0] =~ /D/) {
						if ($riga[1] =~ /^\s(?:1|0):1:\s{11}:(?:\d\d)((?:\+|\-|\d){6})((?:\+|\-|\d){10})$/) {
							$v_totale_sconto = $2*1;
						}
					
					}
				}
				
				#per ogni riga calcolo il valore dello sconto
				for my $i (0 .. $#ar_articolo) {
					if (($v_totale_importo*$v_totale_sconto)==0) {
						print "\n";
					}
					
					push(@ar_sconto, int($ar_importo[$i]/$v_totale_importo*$v_totale_sconto));
					if ($ar_articolo[$i] eq $movimento{'codice_ean'}) {
						$id_record_corrente = $i
					}
					if ($ar_importo[$i] gt $ar_importo[$id_record_maggiore]) {
						$id_record_maggiore = $i
					}
				}
				
				#trovo l'eventuale resto dovuto ai troncamenti
				$v_resto_sconto = $v_totale_sconto;
				for my $i (0 .. $#ar_articolo) {
					$v_resto_sconto -= $ar_sconto[$i]
				}
				
				#aggiungo il resto al record più grande
				$ar_sconto[$id_record_maggiore] += $v_resto_sconto;
				
				#ed assegno alla variabile globale $v.. il valore che corrisponde al record corrente
				$v_quota_sconto_set = $ar_sconto[$id_record_corrente] ;
			 }
		 }
		
	}
    
}

sub F_Record() {

    my $info_aggiuntive    ;
    my $non_utilizzato     ;
    my $contatore_articoli ;
    my $matricola_fiscale  ;

    $tipo_ultima_vendita = 0;
    $reparto_ultima_vendita = 0;

    if ($dbrecord{'body'}    =~ /^(.{16}):(.{2})(.{6})(.{10})$/) {
        $info_aggiuntive     = $1;
        $non_utilizzato      = $2;
        $contatore_articoli  = $3;
        $matricola_fiscale   = $4; # questo campo rappresenta il totale scontrino
    }
    else {
        return 0;
    }

    $matricola_fiscale =~ s/-//ig;

	$movimento{'tipo_operazione'} 	= 0;
	$movimento{'movimento'}			= 20;
	$movimento{'codice_ean'}		= "";
	$movimento{'flag_reparto'}		= "";
	$movimento{'reparto_cassa'}		= "";
	$movimento{'valore_operazione'}	= $matricola_fiscale*1;
	$movimento{'unita_misura'}		= 0;
	$movimento{'quantita'}			= 0;
	$movimento{'codice_campagna'}	= ""; #ok
	$movimento{'codice_promozione'}	= "";
	$movimento{'numero_set'}		= 0;
	$movimento{'codice_prodotto'}	= "";
	$movimento{'carta'}				= "";
	$movimento{'tipo_acquisizione'}	= 0;
}

sub C_Record() {
	my $articolo     ;
	my $ammontare    ;
	my $periodo = &PeriodoPromozionale($dbrecord{'data'});

	if ($dbrecord{'body'} =~ /^(P|D)(0|1):((\s|\d){13})((\-|\+|\d){5})(\.|0)((\d){3})(\+|\-|\<|\>)((\d){9})$/) {
		$articolo  	= $3;
		$ammontare  	= $10.$11;
	} else {
		return 0;
	}

	$movimento{'codice_campagna'} 		= ""; #todo
	$movimento{'codice_promozione'}		= ""; #todo

	$articolo =~ s/\s//ig; # rimozione degli spazi (EAN)

	# MIX-MATCH
	if ($dbrecord{'code3'} eq 5) {
		#$movimento{'movimento'}	    = 8;
		$articolo = ""; # nel caso di mix match il campo contiene il codice del mix
		$movimento{'flag_reparto'}  = 'N';
		$movimento{'movimento'}			= 13;
		($movimento{'codice_campagna'},$movimento{'codice_promozione'}) = &GetCodicePromozione('13',$dbrecord{'data'},'',$dbrecord{'socneg'},20);
	} elsif (($dbrecord{'code3'} eq 2) ||  ($dbrecord{'code3'} eq 3)) {    # SCONTO ARTICOLO
		# se il record i della vendita è di tipo 4 allora l'articolo è un articolo target (campagna corrente)
		if ($promo_list =~ /C0027/ or $promo_list =~ /C0024/) {
			$movimento{'movimento'}		= 94;
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('94',$dbrecord{'data'},$articolo,$dbrecord{'socneg'});
		} elsif ($promo_list =~ /C0493/) {
			$movimento{'movimento'}		= 91;
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('91',$dbrecord{'data'},$articolo,$dbrecord{'socneg'});
				
			# se non trovo campagna e promozione trasformo il movimento in sconto mass market
			if ($movimento{'codice_campagna'} =~ /     /) {
				my $percentuale = 0;
				if ($valore_ultima_vendita != 0) {
					$percentuale = int(abs($ammontare/$valore_ultima_vendita));
					$movimento{'movimento'}			= 13;
					($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
						&GetCodicePromozione('13',$dbrecord{'data'},'',$dbrecord{'socneg'},$percentuale);
					
				}
			}
		} elsif ($promo_list =~ /C0492/) {
			my $percentuale = 50;
			if ($ean_ultima_vendita =~ /998011000(10)\d\d/) {
				$percentuale = 10;
			} elsif ($ean_ultima_vendita =~ /998011000(20)\d\d/) {
				$percentuale = 20;
			} elsif ($ean_ultima_vendita =~ /998011000(30)\d\d/) {
				$percentuale = 30;
			} elsif ($ean_ultima_vendita =~ /998011000(40)\d\d/) {
				$percentuale = 40;
			} elsif ($ean_ultima_vendita =~ /998011000(50)\d\d/) {
				$percentuale = 50;
			}
			$movimento{'movimento'}			= 13;
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
						&GetCodicePromozione('13',$dbrecord{'data'},'',$dbrecord{'socneg'},$percentuale);
			
		} else {
		    $movimento{'movimento'}		= 91;
				($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =	&GetCodicePromozione('91',$dbrecord{'data'},$articolo,$dbrecord{'socneg'});
				
			# se non trovo campagna e promozione trasformo il movimento in sconto mass market
			if ($movimento{'codice_campagna'} =~ /     /) {
				my $percentuale = 0;
				if ($valore_ultima_vendita != 0) {
					$percentuale = int(abs($ammontare/$valore_ultima_vendita*100));
					$movimento{'movimento'}			= 13;
					
					($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
						&GetCodicePromozione('13',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'}, $percentuale);					
				}
			}
		}
		
		if ($articolo eq "") {# sconto su una vendita a reparto
			$movimento{'flag_reparto'} = 'Y';
		} else {
			$movimento{'flag_reparto'} = 'N';
		}
	} elsif (($dbrecord{'code3'} eq 'A')) {# SCONTO BARCODE (BOLLONE)
		$movimento{'movimento'}         = 91;
		if ($articolo eq "") {# sconto su una vendita a reparto
			$movimento{'flag_reparto'} = 'Y';
		} else {
			$movimento{'flag_reparto'} = 'N';
		}
		$movimento{'codice_campagna'}	= '3'.sprintf("%02d",$periodo).'91'; 
		$movimento{'codice_promozione'}	= '99030'.sprintf("%02d",$periodo).'91'; 
	} elsif (($dbrecord{'code2'} eq 5) ||  ($dbrecord{'code2'} eq 6)) {# SCONTO A REPARTO
		$movimento{'movimento'}	= 85;
		$articolo =~ s/\s//ig; # rimozione degli spazi (EAN)
		if ($articolo eq "") { # sconto a reparto su una vendita a reparto
			$movimento{'flag_reparto'} = 'Y';
		} else {
			$movimento{'flag_reparto'} = 'N';
		}
		($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
			&GetCodicePromozione('85',$dbrecord{'data'},'',$dbrecord{'socneg'});
		#$movimento{'codice_campagna'}	= '3'.sprintf("%02d",$periodo).'85'; 
		#$movimento{'codice_promozione'}	= '99030'.sprintf("%02d",$periodo).'85'; 
	} elsif (($dbrecord{'code2'} eq 7) ||  ($dbrecord{'code2'} eq 8)) {# SCONTO TRANSAZIONALE
		# bisogna inserire il record della vendita dell'articolo zero
		$articolo_zero = 1;
		#$movimento{'movimento'}			= 86;
		#si tratta dello sconto dipendente tessera 049....
		$movimento{'movimento'}			= 13;
		($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
						&GetCodicePromozione('13',$dbrecord{'data'},'',$dbrecord{'socneg'}, '10');
		$articolo =~ s/\s//ig; # rimozione degli spazi (EAN)
		if ($articolo eq "") {# sconto a reparto su una vendita a reparto
			$movimento{'flag_reparto'} = 'N';
		} else {
			$movimento{'flag_reparto'} = 'N';
		}
	} else {
		print $log_file_handler "Record C con code2 o code3 non previsti: $dbrecord{'body'}\n";
	}

	if($tipo_ultima_vendita =~ /0/) {# vendita
		$movimento{'tipo_operazione'} = 0;
		if($ammontare*1 > 0) {
			$movimento{'tipo_operazione'} = 1;
		}
	} elsif ($tipo_ultima_vendita =~ /2/) {# vendita negativa
		$movimento{'tipo_operazione'} = 0;
		if($ammontare*1 > 0) {
			$movimento{'tipo_operazione'} = 1;
		}
	} elsif ($tipo_ultima_vendita =~ /4/) {# reso
		$movimento{'tipo_operazione'} = 1;
		if($ammontare*1 < 0) {
			$movimento{'tipo_operazione'} = 0;
		}
	} elsif ($tipo_ultima_vendita =~ /7/) {# storno
		$movimento{'tipo_operazione'} = 1;
		if($ammontare*1 < 0) {
			$movimento{'tipo_operazione'} = 0;
		}
	} elsif ($tipo_ultima_vendita =~ /8/) {# annullo
		$movimento{'tipo_operazione'} = 1;
		if($ammontare*1 < 0) {
			$movimento{'tipo_operazione'} = 0;
		}
	} else {
		$movimento{'tipo_operazione'} = 0;
	}

	$ammontare =~ s/-//ig;

	$movimento{'codice_ean'}		= $articolo;
	$movimento{'reparto_cassa'}		= $dbrecord{'code4'};
	$movimento{'valore_operazione'}		= $ammontare*1;
	$movimento{'unita_misura'}		= 0;
	$movimento{'quantita'}			= 0;
	#$movimento{'codice_campagna'}		= ""; #todo
	#$movimento{'codice_promozione'}	= ""; #todo
	$movimento{'numero_set'}		= 0;
	$movimento{'codice_prodotto'}		= $dbrecord{'articolo'};
	$movimento{'carta'}			= "";
	$movimento{'tipo_acquisizione'}		= 0;
}

sub D_Record() {
	my $articolo;
	my $quantita;
	my $ammontare;
	if ($dbrecord{'body'}    =~ /^\s(0|1):(0|1):\s{11}:(\d\d)((?:\+|\-|\d){6})((?:\+|\-)\d{9})$/) {
		$quantita		= $4;
		$ammontare  = $5;
	} else {
		return 0;
	}
	$ammontare =~ s/-//ig;
}

sub G_Record() {
	my $articolo        ;
	my $action_code     ;
	my $ammontare_punti ;
	my $periodo = &PeriodoPromozionale($dbrecord{'data'});

	if ($dbrecord{'body'}   =~ /^(P|D|\s)(0|1):((\d|\s){13}):((\d){2})((\d|\s|\+|\-){6})((\d|\s|\+|\-){10})$/) {
		$articolo           	= $3;
		$action_code        	= $5;
		$ammontare_punti	= $7;

		$ammontare_punti 	=~s/-//ig;
	} else {
		return 0;
	}

	$articolo =~ s/\s//ig;

	$movimento{'tipo_operazione'}		= 0;
	$movimento{'movimento'}				= 0;
	$movimento{'codice_ean'}			= "";
	$movimento{'flag_reparto'}			= "";
	$movimento{'reparto_cassa'}			= "";
	$movimento{'valore_operazione'}		= 1*$ammontare_punti;
	$movimento{'unita_misura'}			= 0;
	$movimento{'quantita'}				= 0;
	$movimento{'codice_campagna'}		= $campagna_attuale;
	$movimento{'codice_promozione'}		= "";
	$movimento{'numero_set'}			= 0;
	$movimento{'codice_prodotto'}		= "";
	$movimento{'carta'}					= "";
	$movimento{'tipo_acquisizione'}		= 0;

    if ($dbrecord{'code3'} eq 1) {
        $movimento{'codice_campagna'}	= $campagna_attuale;
    } elsif ($dbrecord{'code3'} eq 2) {
        $movimento{'codice_campagna'}	= $campagna_precedente;
    }
    else {
        print $log_file_handler "Code3 $dbrecord{'code3'} non previsto\n";
    }

	if (($dbrecord{'code2'} eq 1) || ($dbrecord{'code2'} eq 3) || ($dbrecord{'code2'} eq 6)) {
		$movimento{'flag_reparto'} = 'N';
		if ($articolo eq '') {
			$movimento{'flag_reparto'} = 'Y';
		}
		
		if ($promo_list =~ /G0022/) {      					# item point
			$movimento{'movimento'}         = 89;
			$movimento{'codice_ean'}	= $articolo;
			$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
			$movimento{'reparto_cassa'} 	= $reparto_ultima_vendita;

			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('89',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'},'');
				
			if ($promo_list =~ /G0505/) {
				$promozione_0505	= 1;
			}
		}
		
		if (($promo_list eq '') && ($promozione_0505)) {# item point aggiuntivi (sempre dopo 0022)
			$movimento{'movimento'}         = 89;
			$movimento{'codice_ean'}	= $articolo;
			$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
			$movimento{'reparto_cassa'} 	= $reparto_ultima_vendita;
			
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('89',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'},'');
			
			$promozione_0505 = 0;
		}
		
		if (($promo_list =~ /G0027/ or $promo_list =~ /C0024/) && ($dbrecord{'code2'} eq 3)) {# item point
			$movimento{'movimento'}         = 93;
			$movimento{'codice_ean'}	= $articolo;
			$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
			$movimento{'reparto_cassa'} 	= $reparto_ultima_vendita;
			
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('93',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'},'');
		}
		
		if (($promo_list =~ /G9927/) && ($dbrecord{'code2'} eq 3)) {# item point
			$movimento{'movimento'}         = 93;
			$movimento{'codice_ean'}	= $articolo;
			$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
			$movimento{'reparto_cassa'} 	= $reparto_ultima_vendita;
			
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('93',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'},'DONO');
				
			$riga_C_virtuale = 1;
		}
		
		if (($promo_list =~ /G9927/) && ($dbrecord{'code2'} eq 1)) {# item point
			$movimento{'movimento'}         = 77;
			$movimento{'codice_ean'}	= $articolo;
			$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
			$movimento{'reparto_cassa'} 	= $reparto_ultima_vendita;
			
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('77',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'},'DONO');
		}
		
		if (($promo_list =~ /G0023/) && (($dbrecord{'code2'} eq 3) || ($dbrecord{'code2'} eq 6))) {      	# item point
			$movimento{'movimento'}         = 90;
			$movimento{'codice_ean'}	= $articolo;
			$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
			$movimento{'reparto_cassa'} 	= $reparto_ultima_vendita;
			$movimento{'carta'}		= $dbrecord{'tessera'};
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('90',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'});
		}
		
		# tipo operazione: eredita quella della vendita
		if($tipo_ultima_vendita =~ /0/) {
			$movimento{'tipo_operazione'} = 0; 		# vendita
		} elsif ($tipo_ultima_vendita =~ /2/) {
			$movimento{'tipo_operazione'} = 0; 		# vendita negativa
		} elsif ($tipo_ultima_vendita =~ /4/) {
			$movimento{'tipo_operazione'} = 1; 		# reso
		} elsif ($tipo_ultima_vendita =~ /7/) {
			$movimento{'tipo_operazione'} = 1; 		# storno
		} elsif ($tipo_ultima_vendita =~ /8/) {
			$movimento{'tipo_operazione'} = 1; 		# annullo
		} else {
			$movimento{'tipo_operazione'} = 0;
		}
	} elsif ($dbrecord{'code2'} eq 2) { 
		$movimento{'movimento'} = 77;				# punti transazione
		($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('77',$dbrecord{'data'},'',$dbrecord{'socneg'},'');
		#$movimento{'codice_campagna'}	= '10202';
		#$movimento{'codice_promozione'}	= '990000410';
		$articolo_zero 		= 0;				# prima di scrivere questo movimento occorre inserire la vendita del record vuoto
	} elsif ($dbrecord{'code2'} eq 4) { 
		$movimento{'movimento'} = 74;				# punti a reparto
		($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('74',$dbrecord{'data'},'',$dbrecord{'socneg'});
		#$movimento{'codice_campagna'}	= '3'.sprintf("%02d",$periodo).'74';
		#$movimento{'codice_promozione'}	= '99030'.sprintf("%02d",$periodo).'74';
	} elsif($dbrecord{'code2'} eq 5) {
		$movimento{'movimento'} = 92;				# totale punti secondario
		($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('92',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'});
		#$movimento{'codice_campagna'}	= '3'.sprintf("%02d",$periodo).'92';
		#$movimento{'codice_promozione'}	= '99030'.sprintf("%02d",$periodo).'92';
    }
}

sub m_Record() {
	$promo_list	= '';
}

# creo un movimento di vendita dell'articolo 0000000000 per i benefici transazionali
sub ArticoloZero() {
	my($ref_dettaglio, @other) = @_;
	undef(%movimento);
	$articolo_zero = 0;

	$movimento{'tipo_operazione'} 		= 0;
	$movimento{'movimento'}			= 1;
	$movimento{'codice_ean'}		= "0000000000000";
	$movimento{'flag_reparto'}		= "N";
	$movimento{'reparto_cassa'}		= "0001";
	$movimento{'valore_operazione'}		= 0;
	$movimento{'unita_misura'}		= 0;
	$movimento{'quantita'}			= 0;
	$movimento{'codice_campagna'}		= "";
	$movimento{'codice_promozione'}		= "";
	$movimento{'numero_set'}		= 0;
	$movimento{'codice_prodotto'}		= "0000000000";
	$movimento{'carta'}			= "";
	$movimento{'tipo_acquisizione'}		= 0;

	$$ref_dettaglio = sprintf("%01d%02d%13s%1s%4s%09d%01d%09d%-9s%-9s%02d%-10s%13s%01d",
							$movimento{'tipo_operazione'},
							$movimento{'movimento'},
							$movimento{'codice_ean'},
							$movimento{'flag_reparto'},
							$movimento{'reparto_cassa'},
							$movimento{'valore_operazione'},
							$movimento{'unita_misura'},
							$movimento{'quantita'},
							$movimento{'codice_campagna'},
							$movimento{'codice_promozione'},
							$movimento{'numero_set'},
							$movimento{'codice_prodotto'},
							$movimento{'carta'},
							$movimento{'tipo_acquisizione'});
}

sub Sconto_0061() {
	my($ref_dettaglio, $totale_sconto_0061, @other) = @_;
	
	my $periodo = &PeriodoPromozionale($dbrecord{'data'});

	undef(%movimento);

	$movimento{'flag_reparto'}		= 'N';
	$movimento{'codice_ean'}		= '';
	$movimento{'codice_prodotto'}	= '';
	$movimento{'tipo_operazione'} 	= 0;
	if ($totale_sconto_0061 > 0) {
		$movimento{'tipo_operazione'} = 1;
	}
	$movimento{'movimento'}			= 51;
	$movimento{'reparto_cassa'}		= '0000';
	$movimento{'valore_operazione'}	= abs($totale_sconto_0061);
	$movimento{'unita_misura'}		= 0;
	$movimento{'quantita'}			= 0;
	$movimento{'numero_set'}		= 0;
	$movimento{'carta'}				= "";
	$movimento{'tipo_acquisizione'}	= 0;
	($movimento{'codice_campagna'},$movimento{'codice_promozione'}) = &GetCodicePromozione('51',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'}, $categoria_tessera);
	
	$$ref_dettaglio = sprintf("%01d%02d%13s%1s%4s%09d%01d%09d%-9s%-9s%02d%-10s%13s%01d",
							$movimento{'tipo_operazione'},
							$movimento{'movimento'},
							$movimento{'codice_ean'},
							$movimento{'flag_reparto'},
							$movimento{'reparto_cassa'},
							$movimento{'valore_operazione'},
							$movimento{'unita_misura'},
							$movimento{'quantita'},
							$movimento{'codice_campagna'},
							$movimento{'codice_promozione'},
							$movimento{'numero_set'},
							$movimento{'codice_prodotto'},
							$movimento{'carta'},
							$movimento{'tipo_acquisizione'});
}

sub Sconto_0481() {
	my($ref_dettaglio, $sconto_0481, $reparto, @other) = @_;
	
	my $periodo = &PeriodoPromozionale($dbrecord{'data'});

	undef(%movimento);

	$movimento{'flag_reparto'}		= 'N';
	$movimento{'codice_ean'}		= '';
	$movimento{'codice_prodotto'}	= '';
	$movimento{'tipo_operazione'} 	= 0;
	if ($sconto_0481 > 0) {
		$movimento{'tipo_operazione'} = 1;
	}
	$movimento{'movimento'}			= 85;
	$movimento{'reparto_cassa'}		= $reparto;
	$movimento{'valore_operazione'}	= abs($sconto_0481);
	$movimento{'unita_misura'}		= 0;
	$movimento{'quantita'}			= 0;
	$movimento{'numero_set'}		= 0;
	$movimento{'carta'}				= "";
	$movimento{'tipo_acquisizione'}	= 0;
	($movimento{'codice_campagna'},$movimento{'codice_promozione'}) = &GetCodicePromozione('85',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'}, $reparto);
	
	$$ref_dettaglio = sprintf("%01d%02d%13s%1s%4s%09d%01d%09d%-9s%-9s%02d%-10s%13s%01d",
							$movimento{'tipo_operazione'},
							$movimento{'movimento'},
							$movimento{'codice_ean'},
							$movimento{'flag_reparto'},
							$movimento{'reparto_cassa'},
							$movimento{'valore_operazione'},
							$movimento{'unita_misura'},
							$movimento{'quantita'},
							$movimento{'codice_campagna'},
							$movimento{'codice_promozione'},
							$movimento{'numero_set'},
							$movimento{'codice_prodotto'},
							$movimento{'carta'},
							$movimento{'tipo_acquisizione'});
}

sub ScontoSet() {
	my($ref_dettaglio, @other) = @_;
	
	my $periodo = &PeriodoPromozionale($dbrecord{'data'});

	undef(%movimento);
	 
	my $flag_farmaceutico   ;
	my $codice_articolo     ;
	my $qta                 ;
	my $decimale            ;
	my $decimali_pxc        ;
	my $segno               ;
	my $valore              ;
	if ($dbrecord{'body'}   =~ /^((A|\d|\s){16})((\+|\-|\d){5})((0|\.){1})(\d{3})((\*|\+|\-|\d){1})(\d{9})$/) {
		$codice_articolo    = $1;
		$qta                = $3;
		$decimale           = $5;
		$decimali_pxc       = $7;
		$segno              = $9;
		$valore             = $10;
		if ($segno eq '*') {$valore = "+".$valore} else {$valore = $segno.$valore};
	} else {
		print "Record S anomalo: $dbrecord{'body'} \n";
		return 0;
	}

	$codice_articolo =~ s/\s//ig; # rimozione di tutti gli spazi
	if ($codice_articolo eq "") { # vendita a reparto
		$movimento{'flag_reparto'}	= "Y";
		$movimento{'codice_ean'}	= "";
		$movimento{'codice_prodotto'}	= "";
	} else {
		$movimento{'flag_reparto'}	= "N";
		$movimento{'codice_ean'}	= $codice_articolo;
		$movimento{'codice_prodotto'}	= $dbrecord{'articolo'};
	}
	
	$movimento{'tipo_operazione'} 		= 1;
	if ($v_quota_sconto_set lt 0) {
		$movimento{'tipo_operazione'} 	= 0;
	}
	
	
	$movimento{'movimento'}			= 91;
	$movimento{'reparto_cassa'}		= $dbrecord{'code4'};
	$movimento{'valore_operazione'}	= abs($v_quota_sconto_set);
	$movimento{'unita_misura'}		= 0;
	$movimento{'quantita'}			= 0;
	#$movimento{'codice_campagna'}		= "11111";
	#$movimento{'codice_promozione'}		= "999999999";
	$movimento{'numero_set'}		= 0;
	$movimento{'carta'}			= "";
	$movimento{'tipo_acquisizione'}		= 0;
	($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
		&GetCodicePromozione('91',$dbrecord{'data'},$movimento{'codice_ean'},$dbrecord{'socneg'});
		
	# se non trovo campagna e promozione trasformo il movimento in sconto mass market
	if ($movimento{'codice_campagna'} =~ /00000|\s{5}/) {
		my $percentuale = 0;
		if ($valore_ultima_vendita != 0) {
			$percentuale = int(abs($movimento{'valore_operazione'}/$valore_ultima_vendita));
			$movimento{'movimento'}			= 13;
			($movimento{'codice_campagna'},$movimento{'codice_promozione'}) =
				&GetCodicePromozione('13',$dbrecord{'data'},'',$dbrecord{'socneg'}, $percentuale);
		}
	}
	
	#print $dbrecord{numcassa}."-".$dbrecord{transazione}."-".$dbrecord{riga}."-".$dbrecord{tiporec}."\n";
	$$ref_dettaglio = sprintf("%01d%02d%13s%1s%4s%09d%01d%09d%-9s%-9s%02d%-10s%13s%01d",
							$movimento{'tipo_operazione'},
							$movimento{'movimento'},
							$movimento{'codice_ean'},
							$movimento{'flag_reparto'},
							$movimento{'reparto_cassa'},
							$movimento{'valore_operazione'},
							$movimento{'unita_misura'},
							$movimento{'quantita'},
							$movimento{'codice_campagna'},
							$movimento{'codice_promozione'},
							$movimento{'numero_set'},
							$movimento{'codice_prodotto'},
							$movimento{'carta'},
							$movimento{'tipo_acquisizione'});

}

# creo un movimento di beneficio relativo allo sconto transazionale
# utilizzo le informazioni del record S
sub ScontoTransazionale() {
    my($ref_dettaglio, @other) = @_;
	$sconto_transazione = 0;
	undef(%movimento);

    # vendita
    if(     $dbrecord{'code2'} =~ /0/) {
            $movimento{'tipo_operazione'} 	= 0;
    }
    if(     $dbrecord{'code2'} =~ /1/) {#aggiunta per eliminare l'errore <- da rivedere perché corrisponde ad un bollone
            $movimento{'tipo_operazione'} 	= 0;
    }
    # vendita negativa
    elsif(  $dbrecord{'code2'} =~ /2/) {
            $movimento{'tipo_operazione'} 	= 0;
    }
    # reso
    elsif(  $dbrecord{'code2'} =~ /4/) {
            $movimento{'tipo_operazione'} 	= 1;
    }
    # storno
    elsif(  $dbrecord{'code2'} =~ /7/) {
            $movimento{'tipo_operazione'} 	= 1;
    }
    # annullo
    elsif(  $dbrecord{'code2'} =~ /8/) {
            $movimento{'tipo_operazione'} 	= 1;
    }

    $movimento{'movimento'}			= 86;


    my $flag_farmaceutico   ;
    my $codice_articolo     ;
    my $qta                 ;
    my $decimale            ;
    my $decimali_pxc        ;
    my $segno               ;
    my $valore              ;
    if ($dbrecord{'body'}   =~ /^((A|\d|\s){16})((\+|\-|\d){5})((0|\.){1})(\d{3})((\*|\+|\-|\d){1})(\d{9})$/) {
        $codice_articolo    = $1;
        $qta                = $3;
        $decimale           = $5;
        $decimali_pxc       = $7;
        $segno              = $9;
        $valore             = $10;
        if ($segno eq '*') {$valore = "+".$valore} else {$valore = $segno.$valore};
    } else {
        print "Record S anomalo: $dbrecord{'body'} \n";
        return 0;
    }


    # codice_articolo
    $codice_articolo =~ s/\s//ig; # rimozione di tutti gli spazi
    if ($codice_articolo eq "") { # vendita a reparto
        $movimento{'flag_reparto'}		= "Y";
        $movimento{'codice_ean'}		= "";
        $movimento{'codice_prodotto'}		= "";
    }
    else {
        $movimento{'flag_reparto'}		= "N";
        $movimento{'codice_ean'}		= $codice_articolo;
        $movimento{'codice_prodotto'}		= $dbrecord{'articolo'};
    }

    my $quota = $dbrecord{'quota_sc_tran'} ;

    # se la quota è > 0 (es: sconto si vendita negativa o articolo buono) inverte il segno dell'operazione
    if($quota*1 >0){ # vendita negativa o articolo buono o storno o reso
        if ($movimento{'tipo_operazione'} == 0) {
#            $movimento{'tipo_operazione'} = 1;
        }
    }
    $quota =~ s/-//ig;

	$movimento{'reparto_cassa'}			= $dbrecord{'code4'};
	$movimento{'valore_operazione'}		= 1*$quota;
	$movimento{'unita_misura'}			= 0;
	$movimento{'quantita'}				= 0;
	$movimento{'movimento'}				= 86;
	($movimento{'codice_campagna'},$movimento{'codice_promozione'}) = &GetCodicePromozione($movimento{'movimento'},$dbrecord{'data'},'',$dbrecord{'socneg'});
	$movimento{'numero_set'}			= 0;
	$movimento{'carta'}				= "";
	$movimento{'tipo_acquisizione'}			= 0;
	#print $dbrecord{numcassa}."-".$dbrecord{transazione}."-".$dbrecord{riga}."-".$dbrecord{tiporec}."\n";
	$$ref_dettaglio = sprintf("%01d%02d%13s%1s%4s%09d%01d%09d%-9s%-9s%02d%-10s%13s%01d",
							$movimento{'tipo_operazione'},
							$movimento{'movimento'},
							$movimento{'codice_ean'},
							$movimento{'flag_reparto'},
							$movimento{'reparto_cassa'},
							$movimento{'valore_operazione'},
							$movimento{'unita_misura'},
							$movimento{'quantita'},
							$movimento{'codice_campagna'},
							$movimento{'codice_promozione'},
							$movimento{'numero_set'},
							$movimento{'codice_prodotto'},
							$movimento{'carta'},
							$movimento{'tipo_acquisizione'});

}

sub GetCodicePromozione_old() {
	my ($tipo_movimento, $data_movimento, $barcode, $negozio, $percentuale) = @_;
	
	my $tipo_promozione = '';
	if ($tipo_movimento eq '89') {
		$tipo_promozione = 'BJ';
	} elsif ($tipo_movimento eq '77' and $percentuale eq 'DONO'){
		$tipo_promozione = 'DJ';
	} elsif ($tipo_movimento eq '91'){
		$tipo_promozione = 'AP';
	} elsif ($tipo_movimento eq '93' and $percentuale eq 'DONO'){
		$tipo_promozione = 'DM';
	} elsif ($tipo_movimento eq '93' and $percentuale ne 'DONO'){
		$tipo_promozione = 'BM';
	}elsif ($tipo_movimento eq '94'){
		$tipo_promozione = 'BM';
	} elsif ($tipo_movimento eq '77' and $percentuale ne 'DONO'){
		$tipo_promozione = 'BT';
	} elsif ($tipo_movimento eq '13'){
		$tipo_promozione = 'MP';
	} elsif ($tipo_movimento eq '90'){
		$tipo_promozione = 'BP';
	} elsif ($tipo_movimento eq '51'){
		$tipo_promozione = 'MT';
	} elsif ($tipo_movimento eq '85'){
		$tipo_promozione = 'RV';
	} elsif ($tipo_movimento eq '86'){
		$tipo_promozione = 'TV';
	}
	
	my $data_promozione 	= '2000-01-01';
	if ($data_movimento 	=~ /^(\d{2})(\d{2})(\d{2})$/) {
		$data_promozione 		= '20'.$1.'-'.$2.'-'.$3;
	}
	
	my $codice_campagna 		= '     ';
	my $codice_promozione 	= '         ';
	my $codice_classe				= '0';
		
	if ($tipo_promozione eq 'BJ' or $tipo_promozione eq 'AP' or $tipo_promozione eq 'BM' or $tipo_promozione eq 'BP' or $tipo_promozione eq 'DM') {
		
		my $codice_articolo = $codice_articolo_ultima_vendita;
		# cerco la promozione
		if ($sth_get_ep_promo_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $codice_articolo, $negozio)) {
			while (my @record = $sth_get_ep_promo_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
			
			if (($tipo_promozione eq 'AP') && ($codice_campagna eq '     ')) {
				$tipo_promozione = 'PF';
				if ($sth_get_ep_promo_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $codice_articolo, $negozio)) {
					while (my @record = $sth_get_ep_promo_ref->fetchrow_array()) {
						$codice_campagna 	= $record[0];
						$codice_promozione	= $record[1];
						$codice_classe		= $record[2];
					}
				}
			}
			
		}
	} elsif ($tipo_promozione eq 'MP') {
		my $parametro_01 = 0;
		if ($percentuale <= 15) {
			$parametro_01 = 10;
		} elsif (($percentuale > 15) && ($percentuale <= 25)){
			$parametro_01 = 20;
		} elsif (($percentuale > 25) && ($percentuale <= 35)){
			$parametro_01 = 30;
		} elsif (($percentuale > 35) && ($percentuale <= 45)){
			$parametro_01 = 40;
		} elsif ($percentuale > 45) {
			$parametro_01 = 50;
		}
		
		# cerco la promozione
		if ($sth_get_ep_promo_2_ref->execute($data_promozione, $data_promozione, 'MP', $parametro_01, $negozio)) {
			while (my @record = $sth_get_ep_promo_2_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
		}
	} elsif ($tipo_promozione eq 'BT' or $tipo_promozione eq 'TV' or $tipo_promozione eq 'TP') {
		if ($sth_get_ep_promo_3_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $negozio)) {
			while (my @record = $sth_get_ep_promo_3_ref->fetchrow_array()) {
				if ($tipo_promozione eq 'BT') {
					#if ($record[0] eq '10363') {
						$codice_campagna 	= $record[0];
						$codice_promozione	= $record[1];
						$codice_classe		= $record[2];
					#}
				} else {
					$codice_campagna 	= $record[0];
					$codice_promozione	= $record[1];
					$codice_classe		= $record[2];
				}
			}
		}
	} elsif ($tipo_promozione eq 'MT') {
		#in questo caso la percentuale contiene il valore della categoria della carta (dipendenti, over 60 etc..)
		if ($negozio =~ /^3/) {$percentuale = 1};
		if ($negozio =~ /^04/) {$percentuale = 1};
		if ($sth_get_ep_promo_4_ref->execute($data_promozione, $data_promozione, 'MT', $percentuale, $negozio)) {
			while (my @record = $sth_get_ep_promo_4_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
		}
	} elsif ($tipo_promozione eq 'RV') {
		if ($sth_get_ep_promo_5_ref->execute($data_promozione, $data_promozione, $tipo_promozione, '0'.$percentuale.'000', $negozio)) {
			while (my @record = $sth_get_ep_promo_5_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
		}
	} elsif ($tipo_promozione eq 'DJ') {
		my $codice_articolo = $codice_articolo_ultima_vendita;
		
		my $codice_campagna_DJ = '';
		if ($sth_get_ep_promo_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $codice_articolo, $negozio)) {
			while (my @record = $sth_get_ep_promo_ref->fetchrow_array()) {
				$codice_campagna_DJ 	= $record[0];
			}
			
			if ($codice_campagna_DJ ne '') {
				if ($sth_get_ep_promo_3_ref->execute($data_promozione, $data_promozione, 'BT', $negozio)) {
					while (my @record = $sth_get_ep_promo_3_ref->fetchrow_array()) {
						if ($codice_campagna_DJ eq $record[0]) {						
							$codice_campagna 	= $record[0];
							$codice_promozione	= $record[1];
							$codice_classe		= $record[2];
						}
					}
				}
			}
		}
	}
	
	return ($codice_campagna, $codice_promozione);
}

sub GetCodicePromozione() {
	my ($tipo_movimento, $data_movimento, $barcode, $negozio, $percentuale) = @_;
	
	my $tipo_promozione = '';
	if ($tipo_movimento eq '89') {
		$tipo_promozione = 'BJ';
	} elsif ($tipo_movimento eq '77' and $percentuale eq 'DONO'){
		$tipo_promozione = 'DJ';
	} elsif ($tipo_movimento eq '91'){
		$tipo_promozione = 'AP';
	} elsif ($tipo_movimento eq '93' and $percentuale eq 'DONO'){
		$tipo_promozione = 'DM';
	} elsif ($tipo_movimento eq '93' and $percentuale ne 'DONO'){
		$tipo_promozione = 'BM';
	}elsif ($tipo_movimento eq '94'){
		$tipo_promozione = 'BM';
	} elsif ($tipo_movimento eq '77' and $percentuale ne 'DONO'){
		$tipo_promozione = 'BT';
	} elsif ($tipo_movimento eq '13'){
		$tipo_promozione = 'MP';
	} elsif ($tipo_movimento eq '90'){
		$tipo_promozione = 'BP';
	} elsif ($tipo_movimento eq '51'){
		$tipo_promozione = 'MT';
	} elsif ($tipo_movimento eq '85'){
		$tipo_promozione = 'RV';
	} elsif ($tipo_movimento eq '86'){
		$tipo_promozione = 'TV';
	}
	
	my $data_promozione 	= '2000-01-01';
	if ($data_movimento 	=~ /^(\d{2})(\d{2})(\d{2})$/) {
		$data_promozione 		= '20'.$1.'-'.$2.'-'.$3;
	}
	
	my $codice_campagna 		= '     ';
	my $codice_promozione 	= '         ';
	my $codice_classe				= '0';
		
	if ($tipo_promozione eq 'BJ' or $tipo_promozione eq 'AP' or $tipo_promozione eq 'BM' or $tipo_promozione eq 'BP' or $tipo_promozione eq 'DM') {
		
		my $codice_articolo = $codice_articolo_ultima_vendita;
		# cerco la promozione
		#if ($sth_get_ep_promo_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $codice_articolo, $negozio)) {
		#	while (my @record = $sth_get_ep_promo_ref->fetchrow_array()) {
		#		$codice_campagna 	= $record[0];
		#		$codice_promozione	= $record[1];
		#		$codice_classe		= $record[2];
		#	}
		#	
		#	if (($tipo_promozione eq 'AP') && ($codice_campagna eq '     ')) {
		#		$tipo_promozione = 'PF';
		#		if ($sth_get_ep_promo_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $codice_articolo, $negozio)) {
		#			while (my @record = $sth_get_ep_promo_ref->fetchrow_array()) {
		#				$codice_campagna 	= $record[0];
		#				$codice_promozione	= $record[1];
		#				$codice_classe		= $record[2];
		#			}
		#		}
		#	}
		#	
		#}
		
		($codice_campagna, $codice_promozione) = getCodicePromozioneAttiva($tipo_promozione, $codice_articolo, '');
		if (($tipo_promozione eq 'AP') && ($codice_campagna eq '     ')) {
			($codice_campagna, $codice_promozione) = getCodicePromozioneAttiva('PF', $codice_articolo, '');
		}	
	} elsif ($tipo_promozione eq 'MP') {
		my $parametro_01 = 0;
		if ($percentuale <= 15) {
			$parametro_01 = 10;
		} elsif (($percentuale > 15) && ($percentuale <= 25)){
			$parametro_01 = 20;
		} elsif (($percentuale > 25) && ($percentuale <= 35)){
			$parametro_01 = 30;
		} elsif (($percentuale > 35) && ($percentuale <= 45)){
			$parametro_01 = 40;
		} elsif ($percentuale > 45) {
			$parametro_01 = 50;
		}
		
		# cerco la promozione
		if ($sth_get_ep_promo_2_ref->execute($data_promozione, $data_promozione, 'MP', $parametro_01, $negozio)) {
			while (my @record = $sth_get_ep_promo_2_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
		}
	} elsif ($tipo_promozione eq 'BT' or $tipo_promozione eq 'TV' or $tipo_promozione eq 'TP') {
		#if ($sth_get_ep_promo_3_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $negozio)) {
		#	while (my @record = $sth_get_ep_promo_3_ref->fetchrow_array()) {
		#		if ($tipo_promozione eq 'BT') {
		#			#if ($record[0] eq '10363') {
		#				$codice_campagna 	= $record[0];
		#				$codice_promozione	= $record[1];
		#				$codice_classe		= $record[2];
		#			#}
		#		} else {
		#			$codice_campagna 	= $record[0];
		#			$codice_promozione	= $record[1];
		#			$codice_classe		= $record[2];
		#		}
		#	}
		#}
		
		($codice_campagna, $codice_promozione) = getCodicePromozioneAttiva($tipo_promozione, '', '');
		
		
	} elsif ($tipo_promozione eq 'MT') {
		#in questo caso la percentuale contiene il valore della categoria della carta (dipendenti, over 60 etc..)
		if ($negozio =~ /^3/) {$percentuale = 1};
		if ($negozio =~ /^04/) {$percentuale = 1};
		if ($sth_get_ep_promo_4_ref->execute($data_promozione, $data_promozione, 'MT', $percentuale, $negozio)) {
			while (my @record = $sth_get_ep_promo_4_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
		}
	} elsif ($tipo_promozione eq 'RV') {
		if ($sth_get_ep_promo_5_ref->execute($data_promozione, $data_promozione, $tipo_promozione, '0'.$percentuale.'000', $negozio)) {
			while (my @record = $sth_get_ep_promo_5_ref->fetchrow_array()) {
				$codice_campagna 	= $record[0];
				$codice_promozione	= $record[1];
				$codice_classe		= $record[2];
			}
		}
	} elsif ($tipo_promozione eq 'DJ') {
		my $codice_articolo = $codice_articolo_ultima_vendita;
		
		my $codice_campagna_DJ = '';
		if ($sth_get_ep_promo_ref->execute($data_promozione, $data_promozione, $tipo_promozione, $codice_articolo, $negozio)) {
			while (my @record = $sth_get_ep_promo_ref->fetchrow_array()) {
				$codice_campagna_DJ 	= $record[0];
			}
			
			if ($codice_campagna_DJ ne '') {
				if ($sth_get_ep_promo_3_ref->execute($data_promozione, $data_promozione, 'BT', $negozio)) {
					while (my @record = $sth_get_ep_promo_3_ref->fetchrow_array()) {
						if ($codice_campagna_DJ eq $record[0]) {						
							$codice_campagna 	= $record[0];
							$codice_promozione	= $record[1];
							$codice_classe		= $record[2];
						}
					}
				}
			}
		}
	}
	
	return ($codice_campagna, $codice_promozione);
}

sub getCodicePromozioneAttiva {
	
	my ($to_find_tipo, $to_find_codice_articolo, $to_find_parametro_01, @others) = @_;
	
	my $codice_campagna = '     ';
	my $codice_promozione = '         ';
	my $codice_classe = '0';
	
	my $chiave = $to_find_tipo.$to_find_codice_articolo.$to_find_parametro_01;
	if (exists($promozioni{$chiave})) {
		$codice_campagna = $promozioni{$chiave}{'campagna'};
		$codice_promozione =$promozioni{$chiave}{'promozione'};
		$codice_classe =$promozioni{$chiave}{'classe'};
	} else {
		print "$chiave\n";
	}
	return ($codice_campagna, $codice_promozione);
}

sub loadPromozioniAttive() {
	my ($data, $negozio, @other) = @_;
	
	if ($data =~ /^(\d{4})(\d{2})(\d{2})$/) {
		$data = $1.'-'.$2.'-'.$3;
	}	
	
	my $sth;
	
	$sth = $dbh->prepare(qq{select p.`codice_campagna`, p.`codice_promozione`, p.`classe`, p.`tipo`,
							case when p.`codice_articolo` <> '0000000' then p.`codice_articolo` else '' end,
							case when p.`tipo` = 'MP' then p.`parametro_01` else '' end 
							from cm.promozioni as p 
							where p.`data_inizio` <= ? and p.`data_fine` >= ? and
							p.`codice_promozione` in (select n.`promozione_codice` from cm.negozi_promozioni as n where n.`negozio_codice` = ?)});
	
	if (!$sth->execute($data, $data, $negozio)) {
        print $log_file_handler "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
	
	%promozioni = ();
	while (my @record = $sth->fetchrow_array()) {
		my $src_codice_campagna = $record[0];
		my $src_codice_promozione = $record[1];
		my $src_classe = $record[2];
		my $src_tipo = $record[3];
		my $src_codice_articolo = $record[4];
		my $src_parametro_01 = $record[5];
		
		$promozioni{$record[3].$record[4].$record[5]} = {'campagna' => $record[0], 'promozione' => $record[1], 'classe' => $record[2]};
    }
	
	my @chiavi = keys %promozioni;
	#print "@chiavi";
	
    return 1;
}

sub GetCodiciCampagna() {
	my($data,@other)=@_;

	my $sth;

    # recupero i codici campagna sulla base della data
    $sth = $dbh->prepare(qq{
        select campagna_attuale, campagna_precedente from ncr.campagne where data_dal <= ? and data_al >= ?;
    });

    if (!$sth->execute($data, $data)) {
        print $log_file_handler "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    while (my @record = $sth->fetchrow_array()) {
		$campagna_attuale 	= $record[0];
		$campagna_precedente	= $record[1];
		#print "$data: attuale:$campagna_attuale\tprecedente:$campagna_precedente\n";
    }
    return 1;
}

sub TestData() {
	my($pdv, $dataTR, $dataDC, $numero_transazione, $numero_cassa, @other)=@_;
	if($dataDC == $dataTR) {
		return 1;
	}
	else {
		my $msg = "Errore sul negozio $pdv: DataDC ($dataDC) <> DataTRANSAZIONE ($dataTR), trans. $numero_transazione cassa $numero_cassa";
		print $log_file_handler "$msg \n";
		&Alert($msg);
		return 0;
	}
}

sub PeriodoPromozionale() {
	my ($data) = @_;
	if (($data >= '130814') && ($data <= '130828')) {
		return 18;
	} elsif (($data >= '130829') && ($data <= '130911')) {
		return 19;
	} else {
		return 0;
	}
}

sub Alert() {
    my($msg,@other) = @_;

	$msg=~s/(\n|\r)/ /ig;

    if (! $dbh) {
        return;
    }

    my $sth = $dbh->prepare(qq{
            INSERT INTO ETL.ALERT ( applicazione,
                                    parametri   ,
                                    tipo        ,
                                    messaggio   ,
                                    gestito     )
			values (
            "datacollect_Epipoli.pl",
            "",
            "ERROR",
            ?,
            0);
			});

    #eseguo la query
    $sth->execute($msg);
}
