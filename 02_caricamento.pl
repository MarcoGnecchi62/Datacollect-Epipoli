#!/perl/bin/perl
use strict;     # pragma che dice all'interprete di essere rigido nel controllo della sintassi
use warnings;   # pragma che dice all'interprete di mostrare eventuali warnings
use DBI;        # permette di comunicare con il database
use File::Find; # permette di effettuare cicli sulle directory

# controllo i paramteri in ingresso
my $dir_riferimento;
my $datadc;

# file di input
my $SOURCE_PATH = "/italmark/etl/ncr/datacollect/hocidc/";
my $LOG_PATH    = "/italmark/log/";

# parametri di configurazione del database
my $database = "ncr";
my $hostname = "127.0.0.1";
my $username = "root";
my $password = "mela";

# variabili globali
my $dbh;
my $sth_insert;
my $linea           = '';
my $line_counter    = 0;

my $now_string  = localtime;
my $time_rx     = time();

my $log_file_handler;
my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $logdate = sprintf("%04d%02d%02d%02d%02d%02d", $year+1900, $mon+1, $mday,$hour, $min, $sec);
my $log_file_name = $LOG_PATH.$logdate."_DCLoader.log";
open $log_file_handler, ">>", $log_file_name; #open log file in append mode

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
if (&ConnessioneDB()) {
    print "Connessione al db :      OK!\n";
}
else {
    die;
}

if (&ReadParams()) {
    $SOURCE_PATH.=lc($dir_riferimento);
    print "Folder di riferimento:   $dir_riferimento\n";
    print "Path da caricare:        $SOURCE_PATH\n";
}
else {
    print "ERRORE!! Impossibile leggere la tabella ncr.parametri!\n";
    die;
}


# Elaborazione dei file
find( \&ElaboraFile, $SOURCE_PATH);


$sth_insert->finish();


my $elab_time   = time() - $time_rx;
$now_string     = localtime;
print "Tempo di elaborazione:   $elab_time secondi\n";
print "Linee analizzate:        $line_counter\n";
print "Fine Elaborazione:       $now_string\n";
close($log_file_handler);

sub ConnessioneDB{
    # connessione al database
    $dbh = DBI->connect("DBI:mysql:ncr:$hostname", $username, $password);
    if (! $dbh) {
        print "Errore durante la connessione al database!\n";
        return 0;
    }
    my $sth;

    # rimozione delle tabella di datacollect
    $sth = $dbh->prepare(qq{
        drop table if exists ncr.datacollect;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    # creazione della tabella di datacollect
    $sth = $dbh->prepare(qq{
        create table if not exists ncr.datacollect (
            id            integer unsigned not null auto_increment,
            datadc        varchar(8)      default null,
            socneg        varchar(4)      default null,
            numcassa      varchar(3)      default null,
            data          varchar(6)      default null,
            ora           varchar(6)      default null,
            transazione   decimal(4,0)    default null,
            riga          decimal(3,0)    default null,
            tiporec       varchar(1)      default null,
            code1         varchar(1)      default null,
            code2         varchar(1)      default null,
            code3         varchar(1)      default null,
            code4         varchar(4)      default null,
            body          varchar(35)     default null,
           primary key (id)
        ) engine=myisam;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    $sth->finish();

    # statement di inserimento di un record nel datacollect
    $sth_insert=    $dbh->prepare(qq{
        insert into ncr.datacollect (
            datadc      ,
            socneg      ,
            numcassa    ,
            data        ,
            ora         ,
            transazione ,
            riga        ,
            tiporec     ,
            code1       ,
            code2       ,
            code3       ,
            code4       ,
            body        )
        values (?,?,?,?,?,?,?,?,?,?,?,?,?);
    });

    return 1;
}

sub ReadParams () {
    my $sth;

    # recupero la lista dei negozi da trattare
    $sth = $dbh->prepare(qq{
        select valore from ncr.parametri where idparametri = 2;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    while (my @record = $sth->fetchrow_array()) {
        $dir_riferimento = $record[0];
    }
    return 1;
}

sub ElaboraFile() {
    my($myfile,@other) = @_;
    my $input_file_name = $File::Find::name;
    $input_file_name =~ s/\\/\//ig;
    # 0187_20100804_100803_dc.txt
    if ($input_file_name =~ /(\d{4})_(\d{8})_(\d{6})_DC\.txt$/i) {
        $datadc = "20".$3;
        print "File in elaborazione:    $input_file_name\n";
        &ReadDataCollect($input_file_name);
    }
}

sub ReadDataCollect() {
    # file di input
    my ($input_file_name, @other) = @_;

    if (!$input_file_name) { # file di input
        die "! Specificare il file da analizzare !\n";
    }
    if (!length($input_file_name)){ # file di input
        die "! Nome del file da analizzare non valido: $input_file_name!\n";
    }

    my $input_file_handler;

    # apro il file in lettura
    if (open $input_file_handler, "<:crlf", $input_file_name) { # il file è aperto con successo

        while(! eof ($input_file_handler))  { #leggo il file, una linea per volta
            $line_counter++;
            $linea = <$input_file_handler>;
            $linea =~ s/\n$//ig;

            if (! &RecordParser(sprintf("%-78s", $linea)) ){
                print $log_file_handler "[$line_counter] Errore Record: $linea\n";
                next;
            }
        }
        close($input_file_handler);
    }
    else { # problemi durante l'apertura del file
       die "Impossibile aprire il file $input_file_name: errore $!\n";
    }
}

sub RecordParser() {
    my($linea, @other) = @_;
    my %record;

    #leggo un record dal file del datacollect
    if ($linea =~ /^(.{32})(\w{1}):(.{44})$/) {
        $record{'header'}           = $1;
        $record{'tipo_record'}      = $2;
        $record{'sottorecord'}      = $3;

        # HEADER
        if ($record{'header'}           =~ /^(\d{4}):(\d{3}):(\d{6}):(\d{6}):(\d{4}):(\d{3}):$/) {
            $record{'negozio'}          = $1;
            $record{'cassa'}            = $2;
            $record{'data'}             = $3;
            $record{'ora'}              = $4;
            $record{'num_transazione'}  = $5;
            $record{'riga_transazione'} = $6;
        }
        else{
            print $log_file_handler "Header anomalo: $record{'header'}\n";
            return 0;
        }

        # sottorecord
        if ($record{'sottorecord'}      =~ /^(.{1})(.{1})(.{1}):(.{4}).(.{35})$/) {
            $record{'code1'} = $1;
            $record{'code2'} = $2;
            $record{'code3'} = $3;
            $record{'code4'} = $4;
            $record{'body'}  = $5;
        }
        else{
            print $log_file_handler "Sottorecord anomalo: $record{'sottorecord'}\n";
            return 0;
        }
    }
    else {
        return 0;
    }

    #return 1;

    # inserimento del record in tabella
    if (!$sth_insert->execute(  $datadc,
                                $record{'negozio'}          ,
                                $record{'cassa'}            ,
                                $record{'data'}             ,
                                $record{'ora'}              ,
                                $record{'num_transazione'}  ,
                                $record{'riga_transazione'} ,
                                $record{'tipo_record'}      ,
                                $record{'code1'}            ,
                                $record{'code2'}            ,
                                $record{'code3'}            ,
                                $record{'code4'}            ,
                                $record{'body'}
                                )) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    return 1;
}
