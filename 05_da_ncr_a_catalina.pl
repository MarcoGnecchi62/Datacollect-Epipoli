#!/perl/bin/perl
#!/perl/bin/perl
use strict;     # pragma che dice all'interprete di essere rigido nel controllo della sintassi
use warnings;   # pragma che dice all'interprete di mostrare eventuali warnings
use DBI;        # permette di comunicare con il database
use File::Find; # permette di effettuare cicli sulle directory 
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use File::Basename;

# parametri di configurazione del database
my $database = "ncr";
my $hostname = "localhost";
my $username = "root";
my $password = "mela";

# variabili globali
my $dbh;
my @negozi; # array di negozi di cui produrre il file per CATALINA
my $line_counter    = 0;    # contatore dei record letti da db
my $now_string  = localtime; 
my $time_rx     = time();

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
    &CreateCATALINAFile($negozio);
}

exit 0;

sub DBConnection{
    # connessione al database
    $dbh = DBI->connect("DBI:mysql:$database:$hostname", $username, $password);
    if ($dbh) {
		return 1;
	} else {
        print "Errore durante la connessione al database!;$!\n";
        return 0;
    } 
    
    #print "Connessione al db:       OK!\n";
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

    my $sth;
    
    # recupero una lista di codici negozi con cui NCR rappresenta i codici nella codifica italbrix
    $sth = $dbh->prepare(qq{
        SELECT DISTINCT neg_ncr FROM ncr.negozio WHERE neg_itm = ?;
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
        drop table if exists catalina.datacollect_negozio;
    });
    
    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    
    $sth = $dbh->prepare(qq{
        create table if not exists catalina.datacollect_negozio like ncr.datacollect_rich;
    });
    
    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    
    
    # $sth = $dbh->prepare(qq{
        # INSERT INTO CATALINA.DATACOLLECT_NEGOZIO SELECT * FROM NCR.DATACOLLECT_RICH WHERE SOCNEG IN (?) AND TIPO_TRANSAZIONE = 1;
    # });
    
    my $query = "insert into catalina.datacollect_negozio select * from ncr.datacollect_rich where socneg in ($codelist) and tipo_transazione = 1;";
    
    #print "$query\n";
    
    $sth = $dbh->prepare($query);
	
    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    else {
        my $righe = $sth->rows;
    }
}


sub CreateCATALINAFile() {
    my($negozio, @other) = @_;
    
    # per un negozio possono essere presenti piu' giorni di datacollect
    # recupero le diverse date di datacollect
    my $sth_datadc =    $dbh->prepare(qq{
                            select distinct datadc from catalina.datacollect_negozio
                        });

    # seleziono le date
    if (!$sth_datadc->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    
    my $datadc;
    while (my @date = $sth_datadc->fetchrow_array()) {
        $datadc = $date[0];
        
		my $catena = '268';
		my $societa = '00';
		if ($negozio =~ /^36/) {
			$catena = '271';
			$societa = '16';
		} elsif ($negozio =~ /^31/) {
			$catena = '271';
			$societa = '01';
		}
		
		#268-0023 ˆ diventa 268-0123
		#271-1654 ˆ diventa 271-3654
		#271-0151 ˆ diventa 268-0151
		#271-0152 ˆ diventa 268-0152
		
		my $codice_negozio = $catena.'.'.$societa.substr($negozio,2,2);
		
        my $filename_old = "/preparazione/file_catalina/it.$catena.$societa".substr($negozio,2,2).".tab.".$datadc."010101.eur";
		if ($codice_negozio eq '268.0023') {
			$codice_negozio = '268.0123'
		} elsif ($codice_negozio eq '271.1654') {
			$codice_negozio = '271.3654'
		} elsif ($codice_negozio eq '271.0151') {
			$codice_negozio = '268.0151'
		} elsif ($codice_negozio eq '271.0152') {
			$codice_negozio = '268.0152'
		}
		my $filename = "/preparazione/file_catalina/it.".$codice_negozio.".tab.".$datadc."010101.eur";
		
        
        # statement di selezione del record 
        my $sth_select_record =    $dbh->prepare(qq{
select      case when NEGOZIO like '01%' and NEGOZIO <> '0123' then lpad(substr(NEGOZIO,3,2),4,0)
				 when NEGOZIO like '0123' then concat('01',substr(NEGOZIO,3,2))
				 when NEGOZIO like '04%' then concat('00',substr(NEGOZIO,3,2)) 
				 when NEGOZIO like '36%' and NEGOZIO <> '3654' then concat('16',substr(NEGOZIO,3,2))
				 when NEGOZIO like '3654' then concat('36',substr(NEGOZIO,3,2))
				 when NEGOZIO like '31%' then concat('01',substr(NEGOZIO,3,2))
				 else NEGOZIO 
				 end as NEGOZIO,
            concat('20',DATA)                   as DATA,
            ORA_TRANSAZIONE                     as ORA,
            lpad(NUMCASSA,3,0)                  as CASSA,
            lpad(substr(CASSIERE,2,3),10,0)     as OPERATORE,
            lpad(   case 
                    when TIPO_PAGAMENTO in ('01|','01|01|') then '001'
                    when TIPO_PAGAMENTO = '02|' then '002'
                    when TIPO_PAGAMENTO in ('03|', '04|') then '003'
                    when TIPO_PAGAMENTO in ('10|','11|','12|','13|','14|','15|','30|','31|','32|','33|','34|') then '004'  
                    when TIPO_PAGAMENTO in ('16|', '35|') then '006'
                    else '005'
                    end
            ,3,0)                               as TIPO_PAGAMENTO,
            lpad(TESSERA,20,0)                  as CARTA_FEDELTA,
            case when binary tiporec = 'm' then lpad(trim(substr(body,13,12)),16,0) else lpad(   case 
                    when BARCODE_ITM = '' 
                    then concat(2,substr(articolo,1,6))
                    else    case 
                            when length(trim(substr(body,1,16))) in (1,2,3,4,5,6,7,10)
                            then trim(substr(body,1,16))
                            else substr(trim(substr(body,1,16)),1,length(trim(substr(body,1,16)))-1)
                            end
                    end
            ,16,0) end                              as UPC,
            case when binary tiporec = 'm' then '0000000001' else lpad(CODE4,10,0) end                    as REPARTO,
            case when binary tiporec = 'm' then '0001' else 
            case when   QTA_VENDUTA < 0
            then        case 
                        when length(trim(substr(body,1,16))) = 13 and substr(trim(substr(body,1,16)),1,5) = 99777 then lpad(truncate(abs(QTA_VENDUTA),0),4,0) 
                        else concat('-',lpad(truncate(case when length(trim(substr(body,1,16))) = 13 and substr(trim(substr(body,1,16)),1,1) = 2 then 1 else abs(QTA_VENDUTA) end,0),3,0))
                        end
            else concat('',lpad(truncate(case when length(trim(substr(body,1,16))) = 13 and substr(trim(substr(body,1,16)),1,1) = 2 then 1 else abs(QTA_VENDUTA) end,0)   ,4,0))
            end end                                  as QUANTITA,
            
            case when TRUNCATE(VALORE_NETTO + QUOTA_SC_TRAN + quota_sc_tran_0061 + quota_sc_rep_0481,0) < 0
            then concat('-',lpad(TRUNCATE(VALORE_NETTO + QUOTA_SC_TRAN + quota_sc_tran_0061 + quota_sc_rep_0481,0)*-1,8,0))
            else concat('',lpad(TRUNCATE(VALORE_NETTO + QUOTA_SC_TRAN + quota_sc_tran_0061 + quota_sc_rep_0481,0),9,0))
            end                                 as VALORE,
            LPAD(TRANSAZIONE,4,0)               as NUM_TRANSAZIONE
from        catalina.datacollect_negozio
WHERE       VALORE_NETTO + QUOTA_SC_TRAN + quota_sc_tran_0061 + quota_sc_rep_0481<> 0 or (binary tiporec = 'm' and (body like '%CAT99%' or body like '%CAT98%' or body like '%CAT97%'))
AND         DATADC = ?});
#into outfile "$filename"
#fields terminated by '\t'
#lines terminated by '\r\n'
#        });
    
		# seleziono i record dalla tabella datacollect
		if ($sth_select_record->execute($datadc)) {
				if (open my $file_handler, "+>:crlf", "$filename" ) {
						while (my @record = $sth_select_record->fetchrow_array()) {
								print $file_handler "$record[0]\t$record[1]\t$record[2]\t$record[3]\t$record[4]\t$record[5]\t$record[6]\t$record[7]\t$record[8]\t$record[9]\t$record[10]\t$record[11]\n";
						}
						close($file_handler)
				}
		} else {
				print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
				return 0;
		}
		
		my($name, $directories, $suffix) = fileparse($filename);
    
		my $zip = Archive::Zip->new();

		if(! $zip->addFile($filename, $name)) {
			print "Errore durante in fase di creazione file ZIP (addFile)\n";
			return;
		}

		my $zipfile_name=$filename;
		$zipfile_name =~ s/\..{3}$/\.zip/ig; 
		$zipfile_name =~ s/it\./al\.it\./ig; 
		
	
		if ($zip->writeToFileNamed($zipfile_name) != AZ_OK) {
			print "Errore durante in fase di creazione file ZIP (writeToFileNamed)\n";
			return;
		} else {
			#print "Creato il file compresso $zipfile_name\n";
			unlink $filename;
		}
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
            "datacollect_CATALINA_asar.pl",
            "",
            "ERROR",
            ?,
            0); 
			});
    
    #eseguo la query
    $sth->execute($msg);
}