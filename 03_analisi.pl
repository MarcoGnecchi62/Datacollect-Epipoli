#!/perl/bin/perl
use strict;     # pragma che dice all'interprete di essere rigido nel controllo della sintassi
use warnings;   # pragma che dice all'interprete di mostrare eventuali warnings
use DBI;        # permette di comunicare con il database
use File::Find; # permette di effettuare cicli sulle directory
use DBI qw(:sql_types);
use List::MoreUtils qw(firstidx);

my $LOG_PATH = "/italmark/log/";

# parametri di configurazione del database
my $database = "ncr";
my $hostname = "127.0.0.1";
my $username = "root";
my $password = "mela";

my $transazione_test = '01340021308220043';

# variabili globali
my $dbh;
my $sth_negozio;
my $sth_barcode;
my $sth_insert_barcode;
my $sth_delete_barcode;
my $sth_insert_beneficio;
my $sth_select_record;
my $sth_abbina_articolo;
my $sth_aggiorna_transazione;
my $sth_aggiorna_reparto;
my $sth_aggiorna_vendita;
my $sth_spalma_sconti_transazionali;
my $sth_spalma_sconti_transazionali_0061;
my $sth_spalma_sconti_reparto_0481;
my $sth_totale_sconti_reparto_0481;
my $sth_tot_sconto_ventilato;
my $sth_tot_sconto_ventilato_0061;
my $sth_tot_sconto_ventilato_0481;
my $sth_select_beneficio_trans;
my $sth_update_beneficio_trans_D;
my $sth_select_beneficio_trans_mD;
my $sth_update_beneficio_trans_G;
my $sth_select_beneficio_semplice;
my $sth_update_beneficio_semplice_m ;
my $sth_update_beneficio_d_G;
my $sth_update_beneficio_d_D;
my $sth_select_d_punti_residui_G;
my $sth_update_d_punti_residui_G ;
my $sth_select_d_sconto_residuo_D;
my $sth_update_d_sconto_residuo_D;
my $sth_estrai_benefici_P;
my $sth_estrai_benefici_T;
my $sth_dati_ventilazione;
my $sth_dati_ventilazione_rep;
my $sth_alloca_residuo_sconto;
my $sth_alloca_residuo_sconto_0061;
my $sth_alloca_residuo_sconto_0481;
my $sth_alloca_residuo_punti;
my $sth_select_max_beneficio_semplice;
my $sth_update_max_beneficio_semplice;

my $sth_estrai_vendite_x_beneficio ;
my $sth_abbina_beneficio_vendita;

my $line_counter    				= 0;    # contatore dei record letti da db
my $id_corrente     				= 0;    # id del record corrente (ultimo record letto)
my $transazione_corrente;
my $sequenza_da_ignorare 			= '';
my $idpromo							= '';
my $id_inizio_transazione   		= 0;
my $id_fine_transazione     		= 0;
my $id_ultima_vendita       		= 0;
my $update_vendita          		= 0;
my $tipo_i                  		= 0;
my $no_promo_flag					= 0;
my $m_transazionale         		= 0;
my $m_semplice              		= 0;
my $valore_coinvolto_totale_d 		= 0;
my $tr_corrente     				= '';
my $dettaglio_in_corso      		= 0;
my $tr_aperta       				= 0;
my $tr_tipo         				= '';
my $tr_tessera      				= '';
my $prev_record     				= '';
my $articolo_corrente  				= '';
my %record;
my $valore_lordo     				= 0;
my $quota_nonpagata  				= 0;
my $unita_vendute  	 				= 0;
my $qta_venduta      				= 0;
my $deprezzato       				= 0;
my $bollone          				= 0;
my $buono_sconto     				= 0;
my $totale_scontrino_progressivo 	= 0;
my $pti_transazione  				= 0;
my $pti_articolo     				= 0;
my $pti_target       				= 0;
my $cassiere         				= '';
my $ora_transazione  				= '';
my $tipo_pagamento   				= '';
my %totale_reparto;
my $data_riferimento;
my $record_counter;
my $codice_negozio_ncr;
my $codice_negozio_itm;
my $riga_transazione;
my @transazione;
my $last_neg = "";

my $now_string  = localtime;
my $time_rx     = time();

my $log_file_handler;
my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $logdate = sprintf("%04d%02d%02d%02d%02d%02d", $year+1900, $mon+1, $mday,$hour, $min, $sec);
my $log_file_name = $LOG_PATH.$logdate."_DCAnalyzer.log";
open $log_file_handler, ">>", $log_file_name; #open log file in append mode

if (0) { # per redirigere lo standard output sul file di log
    open OUTPUT, '>>', $log_file_name or die $!;
    STDOUT->fdopen( \*OUTPUT, 'w' ) or die $!;
}

# per gestire l'autoflush del log
my $default_fh = select($log_file_handler);
$| = 1;
select($default_fh);

#print "Avvio Elaborazione:      $now_string\n";

# stabilisco la connessione con il db di appoggio per il recupero dei dati
if (&DBConnection()) {
}
else {
    die;
}

# Gestione del datacollect
&ReadDataCollect();

$sth_negozio->finish();
$sth_barcode->finish();
$sth_insert_barcode->finish();
$sth_insert_beneficio->finish();
$sth_delete_barcode->finish();
$sth_select_record->finish();
$sth_abbina_articolo->finish();
$sth_aggiorna_transazione->finish();
$sth_aggiorna_reparto->finish();
$sth_aggiorna_vendita->finish();
$sth_spalma_sconti_transazionali->finish();
$sth_spalma_sconti_transazionali_0061->finish();
$sth_spalma_sconti_reparto_0481->finish();
$sth_tot_sconto_ventilato->finish();
$sth_tot_sconto_ventilato_0061->finish();
$sth_tot_sconto_ventilato_0481->finish();

my $elab_time   = time() - $time_rx;
$now_string     = localtime;
#print "Tempo di elaborazione:   $elab_time secondi\n";
#print "Linee analizzate:        $line_counter\n";
#print "Fine Elaborazione:       $now_string\n";
close($log_file_handler);

sub DBConnection{
    # connessione al database
    $dbh = DBI->connect("DBI:mysql:ncr:$hostname", $username, $password);
    if (! $dbh) {
        print "Errore durante la connessione al database!\n";
        return 0;
    }

#    print "Connessione al db:       OK!\n";

    my $sth;

    # rimozione della tabella di datacollect arricchito
    $sth = $dbh->prepare(qq{
        drop table if exists ncr.datacollect_rich;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    # creazione della tabella di datacollect arricchito
    $sth = $dbh->prepare(qq{
        create table if not exists ncr.datacollect_rich like ncr.datacollect;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
#    print "Creazione tabella dati:  OK!\n";

    # AGGIUNGO TUTTI I CAMPI DI ARRICCHIMENTO
    $sth = $dbh->prepare(qq{
        alter table `ncr`.`datacollect_rich`
		add column `longidtransazione`  varchar(17)     not null default ''     ,
		add column `record`             varchar(78)     not null default ''     ,
		add column `tipo_transazione`   integer         not null default 0      ,
		add column `tipo_i`             varchar(3)      not null default ''     ,
		add column `negozio`            varchar(4)      not null default ''     ,
		add column `cassiere`           varchar(4)      not null default ''     ,
		add column `ora_transazione`    varchar(6)      not null default ''     ,
		add column `tipo_pagamento`     varchar(50)     not null default ''     ,
		add column `articolo`           varchar(7)      not null default ''     ,
		add column `tessera`            varchar(13)     not null default ''     ,
		add column `unita_vendute`      decimal(10,0)   not null default 0      ,
		add column `qta_venduta`        decimal(10,4)   not null default 0      ,
		add column `valore_lordo`       decimal(10,0)   not null default 0      ,
		add column `valore_netto`       decimal(10,0)   not null default 0      ,
		add column `tot_scontrino`      decimal(10,0)   not null default 0      ,
		add column `tot_reparto`        decimal(10,0)   not null default 0      ,
		add column `sconto_transaz`     decimal(10,0)   not null default 0      ,
		add column `punti_transaz` 	    decimal(10,0)   not null default 0      ,
		add column `punti_reparto` 	    decimal(10,0)   not null default 0      ,
		add column `quota_sc_tran` 	    decimal(10,0)   not null default 0      ,
		add column `quota_pti_tran`     decimal(10,0)   not null default 0      ,
		add column `plu`        		varchar(13)     not null default ''     ,
		add column `barcode_itm`        varchar(13)     not null default ''     ,
		add column `punti_articolo`     decimal(10,0)   not null default 0      ,
		add column `quota_nonpagata`    decimal(10,4)   not null default 0      ,
		add column `punti_target`       decimal(10,0)   not null default 0      ,
		add column `deprezzato`         decimal(1,0)    not null default 0      ,
		add column `no_promo_flag`      decimal(1,0)    not null default 0      ,
		add column `buono_sconto`       varchar(13)     not null default 0      ,
		add column `promolist`        	varchar(256)    not null default ''		,
		add column `sconto_transaz_0061`decimal(10,0)   not null default 0      ,
		add column `quota_sc_tran_0061` decimal(10,0)   not null default 0      ,
		add column `sconto_rep_0481`    decimal(10,0)   not null default 0      ,
		add column `quota_sc_rep_0481`  decimal(10,0)   not null default 0      ,
		add KEY `longidtransazione` (`longidtransazione`),
		add KEY `tiporec` (`tiporec`),
		add KEY `code1` (`code1`),
		add KEY `code4` (`code4`);
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
#    print "Campi aggiuntivi:        OK!\n";

    $sth = $dbh->prepare(qq{
    insert ncr.datacollect_rich (id,
                                datadc     ,
                                socneg     ,
                                numcassa   ,
                                data       ,
                                ora        ,
                                transazione,
                                riga       ,
                                tiporec    ,
                                code1      ,
                                code2      ,
                                code3      ,
                                code4      ,
                                body       ,
                                record     )
    select distinct             id         ,
                                datadc     ,
                                socneg     ,
                                numcassa   ,
                                data       ,
                                ora        ,
                                transazione,
                                riga       ,
                                tiporec    ,
                                code1      ,
                                code2      ,
                                code3      ,
                                code4      ,
                                body       ,
                                concat(
                                    lpad(socneg,4,0), ":",
                                    lpad(numcassa,3,0),":",
                                    lpad(data,6,0),":",
                                    lpad(ora,6,0),":",
                                    lpad(transazione,4,0),":",
                                    lpad(riga       ,3,0),":",
                                    lpad(tiporec    ,1,0),":",
                                    code1,
                                    code2,
                                    code3,":",
                                    lpad(code4,4,0),":",
                                    lpad(body,35," ")
                                )
    from ncr.datacollect;
    });
    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
#    print "Importazione dati:       OK!\n";


    # rimozione della tabella dei benfici
    $sth = $dbh->prepare(qq{
        drop table if exists ncr.benefici;
    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    # creazione della tabella dei benefici
    $sth = $dbh->prepare(qq{

    create table `ncr`.`benefici` (
      `id`                          integer unsigned not null auto_increment,
	  tipo_record_insert			varchar(1) not null,
      `tipo_beneficio`              varchar(1) not null,
	  livello_beneficio				varchar(1) not null,
      `idpromo`                     varchar(7) not null,
      `codice_promo`                varchar(4) not null,
      `plu`                         varchar(16) not null,
	  `reparto`                     varchar(4) not null,
	  `longidtransazione`           varchar(17) not null,
	  stato							varchar(1) not null,
      `qta_coinvolta_articolo`      decimal(7,2) not null,
      `valore_coinvolto_articolo`   decimal(7,0) not null,
      `valore_coinvolto_totale`     decimal(7,0) not null,
	  `valore_sconto_totale`        decimal(7,0) not null,
      `valore_sconto_articolo`      decimal(7,0) not null,
      `punti_totali`                decimal(5,0) not null,
      `punti_articolo`              decimal(5,0) not null,
      `id_vendita_abbinata`         decimal(15,0) not null,
	  `id_insert`         			decimal(15,0) not null,
	  `id_update`         			decimal(15,0) not null,
	  `id_close`         			decimal(15,0) not null,
      	primary key (`id`),
  KEY `id_insert` (`id_insert`),
  KEY `id_update` (`id_update`),
  KEY `id_close` (`id_close`),
  KEY `longidtransazione` (`longidtransazione`),
  KEY `tipo_beneficio` (`tipo_beneficio`),
  KEY `livello_beneficio` (`livello_beneficio`),
  KEY `plu` (`plu`)
    )
    engine = myisam;

    });

    if (!$sth->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    # statement di creazione nuovo beneficio
    $sth_insert_beneficio =   $dbh->prepare(qq{
        insert into ncr.benefici (
					tipo_record_insert			,
					tipo_beneficio              ,
					livello_beneficio			,
					idpromo                     ,
					codice_promo                ,
					plu                         ,
					reparto                     ,
					longidtransazione           ,
					stato						,
					qta_coinvolta_articolo      ,
					valore_coinvolto_articolo   ,
					valore_coinvolto_totale     ,
					valore_sconto_totale        ,
					valore_sconto_articolo      ,
					punti_totali                ,
					punti_articolo              ,
					id_vendita_abbinata			,
					id_insert					,
					id_update       			,
					id_close
					)
        values      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    });

	# statement di ricerca beneficio semplice a ritroso (x record m)
    $sth_select_beneficio_semplice =   $dbh->prepare(qq{
				select	id,
					id_insert,
					tipo_record_insert
				from	ncr.benefici
				where 	stato <> 'C'
				and	((binary tipo_record_insert = 'G' and tipo_beneficio = 'G')
				or	(binary tipo_record_insert = 'C' and tipo_beneficio = 'C')
				or	(binary tipo_record_insert = 'd'))
				and	livello_beneficio in ('P','D')
				and	id_insert >= ?
	});

	# statement di ricerca beneficio C semplice a ritroso (x record S storno bollone)
    $sth_select_max_beneficio_semplice =   $dbh->prepare(qq{
				select	max(id)
				from	ncr.benefici
				where 	stato				<> 'C'
				and		(binary tipo_record_insert = 'C' and tipo_beneficio = 'C')
				and		id_insert 			>=	?
	});

	# aggiorno il beneficio ritornato dallo statement sth_select_max_beneficio_semplice
	$sth_update_max_beneficio_semplice =   $dbh->prepare(qq{
				update	ncr.benefici
				set		idpromo			= '1111111',
						codice_promo	= '0492',
						stato			= 'C',
						id_close		= ?
				where 	id = ?
	});

	$sth_update_beneficio_semplice_m = $dbh->prepare(qq{
				update 	ncr.benefici
				set		idpromo 		= ?,
						codice_promo 	= ?,
						stato		 	= 'C',
						id_close		= ?
				where	id 				= ?
	});


	# statement di ricerca beneficio transazionale a ritroso (x record D e G)
    $sth_select_beneficio_trans =   $dbh->prepare(qq{
				select	id,
				id_insert
				from	ncr.benefici
				where 	stato				= 'I'
				and		tipo_record_insert  = 'm'
				and		livello_beneficio	= 'T'
				and		id_insert 			>=	?
				and		codice_promo in 	(?)
	});

	# statement di ricerca beneficio transazionale a ritroso (x record m)
    $sth_select_beneficio_trans_mD =   $dbh->prepare(qq{
				select	id
				from	ncr.benefici
				where 	stato				= 'I'
				and		tipo_record_insert  = 'D'
				and		livello_beneficio	= 'T'
				and		id_insert 			>=	?
	});


	# statement di update beneficio transazionale a ritroso (x record D)
    $sth_update_beneficio_trans_D =   $dbh->prepare(qq{
				update	ncr.benefici set tipo_beneficio	= 'D',	stato	= 'C'	, valore_sconto_totale= ?	,
				id_update	= ?, id_close	= ?	where	id	= ?
		});

	# statement di update beneficio transazionale a ritroso (x record G)
    $sth_update_beneficio_trans_G =   $dbh->prepare(qq{
				update	ncr.benefici
				set		tipo_beneficio		= 'G'	,
						stato				= 'C'	,
						punti_totali		= ?		,
						valore_coinvolto_totale = ?	,
						id_update			= ?  	,
						id_close			= ?
				where	id					= ?
	});



	# statement di update beneficio semplice a ritroso su record d (x record G)
    $sth_update_beneficio_d_G =   $dbh->prepare(qq{
				update	ncr.benefici
				set		id_update			= ?	,
						tipo_beneficio		= 'G',
						punti_articolo		= ifnull(truncate(? * (valore_coinvolto_articolo/?),0),0),
						punti_totali		= ?	,
						valore_coinvolto_totale = ?
				where	binary tipo_record_insert  = 'd'
				and		stato 				= 'I'
				and		tipo_beneficio		= ''
				and		livello_beneficio	= 'P'
				and		id_insert			> ?
				and		id_insert			< ?
	});

	# statement x capire a quale dei record d dello stesso record G devono essere assegnati i punti residui non spalmati (x record G)
    $sth_select_d_punti_residui_G =   $dbh->prepare(qq{
				select	max(valore_coinvolto_articolo),
						sum(punti_articolo)
				from	ncr.benefici
				where	id_update = ?
				and		tipo_beneficio = 'G'
				and		tipo_record_insert = 'd'
				group by id_update
	});

	# attribuzione dei punti non ancora distribuiti al record d con quota maggiora
	$sth_update_d_punti_residui_G =   $dbh->prepare(qq{
				update 	ncr.benefici
				set		punti_articolo = punti_articolo + ?
				where	tipo_record_insert 	= 'd'
				and		id_update			= ?
				and		tipo_beneficio		= 'G'
				and		valore_coinvolto_articolo = ?
				limit 	1
	});

	# statement di update beneficio semplice a ritroso su record d (x record D)
    $sth_update_beneficio_d_D =   $dbh->prepare(qq{
				update	ncr.benefici
				set		id_update				= ?  ,
						tipo_beneficio			= 'D',
						valore_sconto_articolo	= ifnull(truncate(? * (valore_coinvolto_articolo/?),0),0),
						valore_sconto_totale	= ?	,
						valore_coinvolto_totale = ?
				where	binary tipo_record_insert  = 'd'
				and		stato 				= 'I'
				and		tipo_beneficio		= ''
				and		livello_beneficio	= 'P'
				and		id_insert			> ?
				and		id_insert			< ?
	});

	# statement x capire a quale dei record d dello stesso record D deve essere assegnato lo sconto residuo non spalmato (x record D)
    $sth_select_d_sconto_residuo_D =   $dbh->prepare(qq{
				select	max(valore_coinvolto_articolo),
						sum(valore_sconto_articolo)
				from	ncr.benefici
				where	id_update = ?
				and		tipo_beneficio = 'D'
				and		tipo_record_insert = 'd'
				group by id_update
	});

	# attribuzione dello sconto non ancora distribuito al record d con quota maggiore
	$sth_update_d_sconto_residuo_D =   $dbh->prepare(qq{
				update 	ncr.benefici
				set		valore_sconto_articolo = valore_sconto_articolo + ?
				where	tipo_record_insert 	= 'd'
				and		id_update			= ?
				and		tipo_beneficio		= 'D'
				and		valore_coinvolto_articolo = ?
				limit 	1
	});

    # statement per il recupero del codice articolo (ordina in modo da tenere per primi gli articoli non eliminati)
    $sth_barcode=   $dbh->prepare(qq{
        select      lpad(`codcin-bar2`,7,0)
        from        archivi.barartx2 as b inner join archivi.articox2 as a on a.`COD-ART2`=b.`CODCIN-BAR2`
        where       b.`bar13-bar2`     = ?
        order by a.`DATELIM-ART2`   
        limit       1
    });

    # statement per il recupero del codice negozio
    $sth_negozio=   $dbh->prepare(qq{
        select      neg_itm
        from        ncr.negozio
        where       neg_ncr     = ?
    });

    # statement per la rimozione dei barcode
    $sth_delete_barcode =   $dbh->prepare(qq{
        delete
        from        ncr.errori_barcode
        where       barcode     = ?
    });

    # statement per l'inserimento dei barcode senza articolo
    $sth_insert_barcode =   $dbh->prepare(qq{
        insert      into ncr.errori_barcode (barcode,reparto,ultima_rilevazione, ultimo_prezzo)
        values      (?,?,?,?/100)
        on duplicate key
        update       ultima_rilevazione = case when ultima_rilevazione < ? then ? else  ultima_rilevazione end, ultimo_prezzo = ?/100;
    });

	# statement di selezione del record
    $sth_select_record =    $dbh->prepare(qq{
        select      id            ,
                    socneg        ,
                    numcassa      ,
                    data          ,
                    ora           ,
                    lpad(transazione,4,0),
                    lpad(riga,3,0),
                    tiporec       ,
                    code1         ,
                    code2         ,
                    code3         ,
                    code4         ,
                    body
        from        ncr.datacollect
		limit		?,?
        });

    $sth_abbina_articolo = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         plu			= trim(?),
					articolo    = ?,
                    barcode_itm = ?
        where       id = ?;
        });

    $sth_aggiorna_transazione = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         longidtransazione	= ?,
					tipo_transazione    = ?,
                    negozio             = ?,
                    tessera             = ?,
                    tot_scontrino       = ?,
                    cassiere            = ?,
                    ora_transazione     = ?,
                    tipo_pagamento      = ?
        where       id >= ?
        and         id <= ?;
        });

	$sth_spalma_sconti_transazionali = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         promolist 		= concat(promolist,"_",?),
					sconto_transaz	= case when no_promo_flag in (0,2) then 0 else sconto_transaz + ? end,
					punti_transaz	= case when no_promo_flag in (0,1) then 0 else punti_transaz + ? end,
					quota_sc_tran 	= quota_sc_tran + case when no_promo_flag in (0,2) then 0 else ifnull(truncate((valore_netto*?)/(?),0),0) end,
					quota_pti_tran 	= quota_pti_tran + case when no_promo_flag in (0,1) then 0 else ifnull(truncate((valore_netto*?)/(?),0),0) end
        where       id >= ?
        and         id <= ?
		and			longidtransazione	= ?
		and			code2 not in (4,7)
		and			tiporec = 's'
        });
	
	$sth_spalma_sconti_transazionali_0061 = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         promolist = concat(promolist,"_",?),
					sconto_transaz_0061	= case when no_promo_flag in (0,2) then 0 else sconto_transaz_0061 + ? end,
					quota_sc_tran_0061 = quota_sc_tran_0061 + case when no_promo_flag in (0,2) then 0 else ifnull(truncate((valore_netto*?)/(?),0),0) end
        where       id >= ?
        and         id <= ?
		and			longidtransazione	= ?
		and			code2 not in (4,7)
		and			tiporec = 's'
        });
	
	$sth_spalma_sconti_reparto_0481 = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         promolist = concat(promolist,"_",?),
					sconto_rep_0481	= case when no_promo_flag in (0,2) then 0 else sconto_rep_0481 + ? end,
					quota_sc_rep_0481 = quota_sc_rep_0481 + case when no_promo_flag in (0,2) then 0 else ifnull(truncate((valore_netto*?)/(?),0),0) end
        where       id >= ?
        and         id <= ?
		and			longidtransazione = ?
		and			code4 = ?
		and			code2 not in (4,7)
		and			tiporec = 's'
        });
	
	$sth_totale_sconti_reparto_0481 = $dbh->prepare(qq{
        select	sconto_rep_0481
		from	ncr.datacollect_rich
        where       id >= ?
        and         id <= ?
		and			longidtransazione = ?
		and			code4 = ?
		and			code2 not in (4,7)
		and			tiporec = 's'
        });
	


	$sth_dati_ventilazione = $dbh->prepare(qq{
		select		sum(case when no_promo_flag in (0,2) then 0 else valore_netto end) as base_sconti_transazionali,
					sum(case when no_promo_flag in (0,1) then 0 else valore_netto end) as base_punti_transazionali,
					max(case when no_promo_flag in (0,2) then 0 else valore_netto end) as vendita_massima_sconto_trans,
					max(case when no_promo_flag in (0,1) then 0 else valore_netto end) as vendita_massima_punti_trans
		from		ncr.datacollect_rich
		where		id >= ?
        and         id <= ?
		and			longidtransazione	= ?
		and			code2 not in (4,7)
		and			tiporec = 's'
		});
	
	$sth_dati_ventilazione_rep = $dbh->prepare(qq{	
		select	code4,
				ifnull(sum(case when no_promo_flag in (0,2) then 0 else valore_netto end),0) as base_sconti_transazionali,
				ifnull(max(case when no_promo_flag in (0,2) then 0 else valore_netto end),0) as vendita_massima_sconto_trans
		from ncr.datacollect_rich
		where id >= ? and id <= ? and longidtransazione	= ?	and tiporec = 's' and code2 not in (4,7)
		group by 1 order by 1
	});

    $sth_tot_sconto_ventilato = $dbh->prepare(qq{
        select      ifnull(sum(quota_sc_tran),0),
                    ifnull(sum(quota_pti_tran),0)
        from        ncr.datacollect_rich
        where       id >= ?
        and         id <= ?
		and			longidtransazione	= ?
		and			code2 not in (4,7)
		and			tiporec = 's';
        });
	
	$sth_tot_sconto_ventilato_0061 = $dbh->prepare(qq{
        select      ifnull(sum(quota_sc_tran_0061),0)
        from        ncr.datacollect_rich
        where       id >= ?
        and         id <= ?
		and			longidtransazione	= ?
		and			code2 not in (4,7)
		and			tiporec = 's';
        });
	
	$sth_tot_sconto_ventilato_0481 = $dbh->prepare(qq{
        select      ifnull(sum(quota_sc_rep_0481),0)
        from        ncr.datacollect_rich
        where       id >= ?
        and         id <= ?
		and			longidtransazione	= ?
		and         code4 = ?
		and			code2 not in (4,7)
		and			tiporec = 's';
        });

    $sth_alloca_residuo_sconto  = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         quota_sc_tran  = quota_sc_tran  + ?
        where       id >= ?
        and         id <= ?
        and         valore_netto = ?
		and			no_promo_flag not in (0,2)
		and			code2 not in (4,7)
		and			tiporec = 's'
        limit       1;
        });
	
	$sth_alloca_residuo_sconto_0061  = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         quota_sc_tran_0061  = quota_sc_tran_0061  + ?
        where       id >= ?
        and         id <= ?
        and         valore_netto = ?
		and			no_promo_flag not in (0,2)
		and			code2 not in (4,7)
		and			tiporec = 's'
        limit       1;
        });
	
	$sth_alloca_residuo_sconto_0481  = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         quota_sc_rep_0481  = quota_sc_rep_0481  + ?
        where       id >= ?
        and         id <= ?
        and         valore_netto = ?
		and         code4 = ?
		and			no_promo_flag not in (0,2)
		and			code2 not in (4,7)
		and			tiporec = 's'
        limit       1;
    });

	$sth_alloca_residuo_punti  = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         quota_pti_tran  = quota_pti_tran  + ?
        where       id >= ?
        and         id <= ?
        and         valore_netto = ?
		and			no_promo_flag not in (0,1)
		and			tiporec = 's'
        limit       1;
        });

    $sth_aggiorna_reparto = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         tot_reparto     = ?
        where       id >= ?
        and         id <= ?
        and         tiporec = 's'
        and         code4   = ?;
        });

    $sth_aggiorna_vendita = $dbh->prepare(qq{
        update      ncr.datacollect_rich
        set         unita_vendute   = ?,
		    qta_venduta     = ?,
                    valore_lordo    = ?,
                    valore_netto    = ?,
                    tipo_i          = ?,
                    punti_articolo  = ?,
                    punti_target    = ?,
                    quota_nonpagata = ?,
                    deprezzato      = ?,
		    no_promo_flag	= ?,
                    buono_sconto    = trim(?)
        where       id  = ?;
        });

	# estrazione di tutti i benfici di tipo P di una transazione
	$sth_estrai_benefici_P = $dbh->prepare(qq{
		select	id,
				tipo_beneficio,
				idpromo,
				codice_promo,
				plu,
				qta_coinvolta_articolo,
				valore_coinvolto_articolo,
				valore_sconto_articolo,
				punti_articolo,
				id_insert
		from	ncr.benefici
		where	longidtransazione = ?
		and	id_insert >= ?
		and	id_close <= ?
		and	livello_beneficio = 'P'
        and tipo_record_insert <> 'd'
	});
    #modificato da Marco 02/04/2015
    
	# estrazione di tutti i benfici di tipo T di una transazione
	$sth_estrai_benefici_T = $dbh->prepare(qq{
		select	id,
				tipo_beneficio,
				idpromo,
				codice_promo,
				valore_coinvolto_totale,
				valore_sconto_totale,
				punti_totali
		from	ncr.benefici
		where	longidtransazione 	= 	?
		and		id_insert			>=	?
		and		id_close			<=	?
		
		and		livello_beneficio	= 	'T'
	});#and 	codice_promo <> '0061'

	# estrazione delle vendite abbinabili ad un beneficio PLU
	# il test verifica o il valore coinvolto (beneficio G) o la qta coinvola (C e D)

	#$sth_estrai_vendite_x_beneficio = $dbh->prepare(qq{
	#					select 	id,
	#							case
	#								when promolist like ?
	#								then substr(promolist,locate(substr(?,2,12),promolist)+12,7)
	#								else '+000000'
	#								end as gia_impegnato,
	#							promolist
	#					from 	ncr.datacollect_rich
	#					where 	id >= ? and	id <= ? and	longidtransazione = ? and binary tiporec = 'S' and tipo_transazione = 1 and
	#							trim(PLU) = trim(?) and
	#							(	(
	#									promolist not like 	?
	#									and	(	(abs(unita_vendute)	>= abs(?) and unita_vendute*? > 0 and ? <> 'G')
	#											or (abs(valore_lordo) >= abs(?) and valore_lordo*? > 0 and ? =  'G')
	#										)
	#								)
	#							or	(
	#									promolist like ?
	#									and	(	(abs(unita_vendute - (substr(promolist,locate(substr(?,2,12),promolist)+12,7)))	>= abs(?) and unita_vendute*? > 0 and ? <> 'G')
	#											or (abs(valore_lordo  - (substr(promolist,locate(substr(?,2,12),promolist)+12,7)))	>= abs(?) and valore_lordo*? > 0 and ? =  'G')
	#										)
	#								)
	#	)
	#	limit	1
	#});
	
	$sth_estrai_vendite_x_beneficio = $dbh->prepare(qq{
						select 	id,
								case
									when promolist like ?
									then substr(promolist,locate(substr(?,2,12),promolist)+12,7)
									else '+000000'
								end as gia_impegnato,
								promolist
						from 	ncr.datacollect_rich
						where 	id >= ? and	id <= ? and	longidtransazione = ? and binary tiporec = 'S' and tipo_transazione = 1 and code2 not in (4,7,8) and
								trim(PLU) = trim(?) and
								( (
										((promolist not like ?) or (promolist like '%9927%'))
										and	(	(abs(unita_vendute)	= abs(?) and unita_vendute*? > 0 and ? <> 'G')
												or (abs(valore_lordo) >= abs(?) and valore_lordo*? >= 0 and ? =  'G')
												)
									)
								or	(
										promolist like ?
										and	(	(abs(unita_vendute - (substr(promolist,locate(substr(?,2,12),promolist)+12,7)))	= abs(?) and unita_vendute*? > 0 and ? <> 'G')
												or (abs(valore_lordo  - (substr(promolist,locate(substr(?,2,12),promolist)+12,7)))	>= abs(?) and valore_lordo*? >= 0 and ? =  'G')
											)
									)
								)
						order by id desc
						limit	1
	});

	$sth_abbina_beneficio_vendita = $dbh->prepare(qq{
		update 	ncr.datacollect_rich
		set 	promolist = case when ? = ?
							then concat(promolist,"_",?)
							else replace(promolist,?,?)
							end,
				deprezzato = case when promolist like '%04921111111%' then 1 else deprezzato end,
				valore_netto = valore_netto + ?
		where 	id = ?
	});

    return 1;
}

sub ReadDataCollect() {
    my(@other) = @_;

	my $sth_count_rows = $dbh->prepare(qq{ select count(*) from ncr.datacollect_rich });
	if (!$sth_count_rows->execute()) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

	my $rows = 0;
	while (my @riga = $sth_count_rows->fetchrow_array()) {
        $rows =  $riga[0];
	}
#	print "Sulla ncr.datacollect_rich ci sono $rows righe\n";

	my $da = 0;
	my $a = 0;

	do {

		$da = $a;
		$a = $da + 1000000;

#		print "Lettura da riga $da a riga $a \n";

		$sth_select_record->bind_param(1,$da, {TYPE => SQL_INTEGER});
		$sth_select_record->bind_param(2,1000000, {TYPE => SQL_INTEGER});


		# seleziono i record dalla tabella datacollect
		if (!$sth_select_record->execute()) {
			print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
			return 0;
		}

		# scorro il result set
		while (my @riga = $sth_select_record->fetchrow_array()) {

			$id_corrente =  $riga[0];
			# ricompongo il record originale
			my $linea =     $riga[1].":".       # SOCNEG
							$riga[2].":".       # NUMCASSA
							$riga[3].":".       # DATA
							$riga[4].":".       # ORA
							$riga[5].":".       # TRANSAZIONE
							$riga[6].":".       # RIGA
							$riga[7].":".       # TIPOREC
							$riga[8].           # CODE1
							$riga[9].           # CODE2
							$riga[10].":".      # CODE3
							$riga[11].":".      # CODE4
							$riga[12];          # BODY

			$line_counter++;
			&Datacollect_Manager($linea);
		}
	}
	while ($a < $rows);

#	print "Lette $rows righe\n";
}

sub Datacollect_Manager() {
    my($linea, @other) = @_;
    undef(%record);

    if (! &RecordParser($linea) ){
        print $log_file_handler "[$line_counter] Errore Record: $linea\n";
        return;
    }

	if($record{'negozio'} !~ /03$/) {
#		return
	}

    if ($record{'tipo_record'} eq '') {
        print $log_file_handler "[$line_counter] Record anomalo: $linea\n";
    }
    elsif ( $record{'tipo_record'} eq 'B'){
        if (! &B_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'C'){
        if (! &C_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'D'){
        if (! &D_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'd'){
        if (! &d_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'F'){
        if (! &F_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'f'){
        if (! &f_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'G'){
        if (! &G_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'H') {
        if (! &H_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'i'){
        if (! &i_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'k'){
        if (! &k_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'm'){
        if (! &m_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'S'){
        if (! &S_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
            &Alert("Record $record{'tipo_record'} anomalo: $linea");
        }
    }
    elsif ( $record{'tipo_record'} eq 'T'){
        if (! &T_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'u'){
        if (! &u_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'V'){
        if (! &V_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'X'){
        if (! &X_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'z'){
        if (! &z_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    elsif ( $record{'tipo_record'} eq 'Z'){
        if (! &Z_Record($linea)) {
            print $log_file_handler "[$line_counter] Record $record{'tipo_record'} anomalo: $linea\n";
        }
    }
    else {
        print $log_file_handler "[$line_counter] Record $record{'tipo_record'} in transazione $tr_tipo non gestito: $linea\n";
    }

    $prev_record = $record{'tipo_record'};
}

sub RecordParser() {
    my($linea, @other) = @_;

    #leggo un record dal file del datacollect
    if ($linea 			=~ /^(.{32})(\w{1}):(.{44})$/) {
        $record{'header'}      	= $1;
        $record{'tipo_record'}	= $2;
        $record{'sottorecord'} 	= $3;

        # HEADER
        if ($record{'header'}           =~ /^(\d{4}):(\d{3}):(\d{6}):(\d{6}):(\d{4}):(\d{3}):$/) {
            $record{'negozio'}          = $1;
            $record{'cassa'}            = $2;
            $record{'data'}             = $3;
            $record{'ora'}              = $4;
            $record{'num_transazione'}  = $5;
            $record{'riga_transazione'}	= $6;
        } else {
            print $log_file_handler "Header anomalo: $record{'header'}\n";
            return 0;
        }

        # sottorecord
        if ($record{'sottorecord'}	=~ /^(.{1})(.{1})(.{1}):(.{4}):(.{35})$/) {
            $record{'code1'} 		= $1;
            $record{'code2'} 		= $2;
            $record{'code3'} 		= $3;
            $record{'code4'} 		= $4;
            $record{'body'}  		= $5;
        } else {
	    print $log_file_handler "Sottorecord anomalo: $record{'sottorecord'}\n";
            return 0;
        }
    } else {
        return 0;
    }

    return 1;
}

sub Check_Transazione {
    my($linea, $transazione_record, @other) = @_;
    if (!$tr_aperta) {
        print $log_file_handler "[$line_counter] La transazione $transazione_record non e' mai stata aperta: $linea\n";
        return 0;
    }
    if ($transazione_record ne $tr_corrente) {
        print $log_file_handler "[$line_counter] Record con transazione ($transazione_record) diversa da quella corrente ($tr_corrente): $linea\n";
        return 0;
    }

    push(@transazione,$linea);
}

sub Apertura_Transazione() {

    # valorizzo alcune variabili globali
    $tr_aperta              		= 1;
    $tr_tessera             		= '';
    $totale_scontrino_progressivo	= 0;
    $pti_transazione        		= 0;
    $quota_nonpagata        		= 0;
    $valore_lordo           		= 0;
    $unita_vendute  				= 0;
    $qta_venduta            		= 0;
    $deprezzato             		= 0;
    $bollone						= 0;
    $m_transazionale        		= 0;
    $m_semplice             		= 0;
    $sequenza_da_ignorare 			= '';
    $tipo_pagamento         		= '';
    $buono_sconto           		= 0;
    $pti_articolo           		= 0;
    $pti_target             		= 0;
    $id_inizio_transazione  		= $id_corrente;
    $tr_corrente            		= $record{'num_transazione'};
    $data_riferimento       		= "20".$record{'data'};
    $codice_negozio_ncr     		= $record{'negozio'};
    $riga_transazione       		= $record{'riga_transazione'};
    undef(%totale_reparto);

    $transazione_corrente = $record{'negozio'}.$record{'cassa'}.$record{'data'}.$record{'num_transazione'};

    $codice_negozio_itm = &Transcode_Negozio($codice_negozio_ncr);
}

sub Chiusura_Transazione() {
	my($totale,@other) = @_;

    # per le transazioni di vendita aggiorno i record sul db con le infogenerali della transazione
    if ($tr_tipo eq 1) {
        if (!$sth_aggiorna_transazione->execute($transazione_corrente,$tr_tipo,$codice_negozio_itm,$tr_tessera,$totale,$cassiere,$ora_transazione,$tipo_pagamento,$id_inizio_transazione,$id_fine_transazione)) {
			print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
			return 0;
		}
		else{
		}
    }

	# tutti i benefici di tipo PLU (P) devono essere ricondotti ad una vendita della transazione
	# estraggo tutti i benefici di tipo P della transazione corrente
	if(!$sth_estrai_benefici_P->execute($transazione_corrente, $id_inizio_transazione, $id_fine_transazione)){
		print "errore in fase di esecuzione dello statement sth_estrai_benefici_P\n";
	} else {
		my $id_beneficio;
		my $tipo_beneficio;
		my $idpromo;
		my $codice_promo;
		my $plu;
		my $qta_coinvolta_articolo;
		my $valore_coinvolto_articolo;
		my $valore_sconto_articolo;
		my $punti_articolo;
		my $id_insert;

		# scorro il result set
		my @dati;
		while (my @riga = $sth_estrai_benefici_P->fetchrow_array()) {
			$id_beneficio				=  $riga[0];
			$tipo_beneficio				=  $riga[1];
			$idpromo					=  $riga[2];
			$codice_promo				=  $riga[3];
			$plu						=  $riga[4];
			$qta_coinvolta_articolo		=  $riga[5];
			$valore_coinvolto_articolo	=  $riga[6];
			$valore_sconto_articolo		=  $riga[7];
			$punti_articolo				=  $riga[8];
			$id_insert					=  $riga[9];

			my $promolistitem = $tipo_beneficio.$codice_promo.$idpromo;
			if ($tipo_beneficio eq 'G'){
				$promolistitem.=sprintf("%+.6d",$valore_coinvolto_articolo);
			} else{
				$promolistitem.=sprintf("%+.6d",$qta_coinvolta_articolo);
			}

			my $test_like = '%'.substr($promolistitem,0,12).'%';
			# print "$promolistitem $test_like\n";

			# il beneficio di tipo P deve essere associato ad un record S
			# Ricerco sulla datacollect_rich i record S di vendita compatibili con il beneficio
			# ho sostituito $id_fine_transazione con $id_insert per cercare la vendita pi vicina al beneficio
			if (!$sth_estrai_vendite_x_beneficio->execute($test_like,$test_like,$id_inizio_transazione,$id_insert,$transazione_corrente,$plu,$test_like,$qta_coinvolta_articolo,
				$qta_coinvolta_articolo,$tipo_beneficio,$valore_coinvolto_articolo,$valore_coinvolto_articolo,$tipo_beneficio,$test_like,$test_like,$qta_coinvolta_articolo,
				$qta_coinvolta_articolo,$tipo_beneficio,$test_like,$valore_coinvolto_articolo,$valore_coinvolto_articolo,$tipo_beneficio
			)) {
				print "errore nell'esecuzione dello statement sth_estrai_vendite_x_beneficio\n";
			} else {
				if (! $sth_estrai_vendite_x_beneficio->rows){ 
					print "!!! Non trovate vendite compatibili con il beneficio $id_beneficio (
select id, case when promolist like \'$test_like\' then substr(promolist,locate(substr(\'$test_like\',2,12),promolist)+12,7) else '+000000' end as gia_impegnato, promolist
from ncr.datacollect_rich
where id >= $id_inizio_transazione and id <= $id_fine_transazione and longidtransazione = $transazione_corrente and binary tiporec = 'S' and tipo_transazione = 1 and trim(PLU) = trim(\'$plu\') and \n((promolist not like \'$test_like\' and((abs(unita_vendute)= abs($qta_coinvolta_articolo) and unita_vendute*$qta_coinvolta_articolo > 0 and \'$tipo_beneficio\' <> 'G') or (abs(valore_lordo) >= abs($valore_coinvolto_articolo) and valore_lordo*$valore_coinvolto_articolo > 0 and \'$tipo_beneficio\' =  'G'))) or\n (promolist like \'$test_like\' and ((abs(unita_vendute - (substr(promolist,locate(substr(\'$test_like\',2,12),promolist)+12,7)))	= abs($qta_coinvolta_articolo) and unita_vendute*$qta_coinvolta_articolo > 0 and \'$tipo_beneficio\' <> 'G') or (abs(valore_lordo  - (substr(promolist,locate(substr(\'$test_like\',2,12),promolist)+12,7)))>= abs($valore_coinvolto_articolo) and valore_lordo*$valore_coinvolto_articolo > 0 and \'$tipo_beneficio\' =  'G')))) limit	1\n\n";
				}
				else{
					# sono state trovate vendite compatibili con il beneficio
					while (my @recordS = $sth_estrai_vendite_x_beneficio->fetchrow_array()) {
						my $id_recordS	=  $recordS[0];
						my $gia_impegnato = $recordS[1];
						my $retrived_promolist = $recordS[2];
						if ($retrived_promolist =~ /G9927/) {
								$gia_impegnato = 0;
						}
						
						#print "$gia_impegnato, $retrived_promolist, $test_like\n";
						my $nuovo_impegnato=0;
						if($gia_impegnato !~ /\d/){
							$gia_impegnato = 0;
						}
						if($tipo_beneficio ne 'G'){
							$nuovo_impegnato = $gia_impegnato+$qta_coinvolta_articolo;
						}
						else{
							$nuovo_impegnato = $gia_impegnato+$valore_coinvolto_articolo;
						}
						my $promolistitem_new = $tipo_beneficio.$codice_promo.$idpromo.sprintf("%+.6d",$nuovo_impegnato);
			#			print "Impegnato $gia_impegnato -> $promolistitem_new \n\n";

            
#                        print "update 	ncr.datacollect_rich
#		set 	promolist = case when $promolistitem = $promolistitem_new
#							then concat(promolist,\"_\",$promolistitem_new)
#							else replace(promolist,$promolistitem,$promolistitem_new)
#							end,
#				deprezzato = case when promolist like '%04921111111%' then 1 else deprezzato end,
#				valore_netto = valore_netto + $valore_sconto_articolo
#		where 	id = $id_recordS\n";
						if(! $sth_abbina_beneficio_vendita->execute(	$promolistitem,
																		$promolistitem_new,
																		$promolistitem_new,
																		$promolistitem,
																		$promolistitem_new,
																		$valore_sconto_articolo,
																		$id_recordS)) {
							print "errore nell'esecuzione dello statement sth_abbina_beneficio_vendita\n";
						}
						else {
							# print "Vendita $id_recordS abbinata a beneficio $id_beneficio ($promolistitem)\n";
							$totale_scontrino_progressivo+=$valore_sconto_articolo;
						}
					}
				}
			}
		}
	}

	# gestione sconti transazionali
	# tutti i benefici di tipo T devono essere spalmati sulle vendite della transazione
	# estraggo tutti i benefici di tipo T della transazione corrente
	if (!$sth_estrai_benefici_T->execute($transazione_corrente, $id_inizio_transazione, $id_fine_transazione)) {
		print "errore in fase di esecuzione dello statement sth_estrai_benefici_T\n";
	} else { # trovati sconti transazionali
		# bisogna ottenere i totali a valore delle sole vendite che partecipano agli sconti
		# e i totali a valore delle sole vendite che partecipano ai punti
		# ed i relativi valori max fra le singole vendite coinvolte
		my $base_sconti_transazionali;
		my $base_punti_transazionali;
		my $vendita_massima_sconto_trans;
		my $vendita_massima_punti_trans;
		if(!$sth_dati_ventilazione->execute($id_inizio_transazione, $id_fine_transazione, $transazione_corrente)){
			print "errore in fase di esecuzione dello statement sth_dati_ventilazione\n";
		} else {
			while (my @riga = $sth_dati_ventilazione->fetchrow_array()) {
				$base_sconti_transazionali 		=  $riga[0];
				$base_punti_transazionali 		=  $riga[1];
				$vendita_massima_sconto_trans 	=  $riga[2];
				$vendita_massima_punti_trans 	=  $riga[3];
			}
		}

		my @ar_reparto							= ();
		my @ar_base_sconti_reparto				= ();
		my @ar_vendita_massima_sconto_reparto	= ();
		if(!$sth_dati_ventilazione_rep->execute($id_inizio_transazione, $id_fine_transazione, $transazione_corrente)){
			print "errore in fase di esecuzione dello statement sth_dati_ventilazione\n";
		} else {
			while (my @riga = $sth_dati_ventilazione_rep->fetchrow_array()) {
				push(@ar_reparto, $riga[0]);
				push(@ar_base_sconti_reparto, $riga[1]);
				push(@ar_vendita_massima_sconto_reparto, $riga[2]);
			}
		}
		
		my $id_beneficio;
		my $tipo_beneficio;
		my $idpromo;
		my $codice_promo;
		my $valore_coinvolto_totale;
		my $valore_sconto_totale;
		my $punti_totali;

		my $somma_sconti_transazionali;
		my $somma_punti_transazionali;
		
		my $somma_sconti_transazionali_0061 = 0;
		my $somma_sconti_transazionali_0481 = 0;
        
        my $promolistitem = '';

		# scorro il result set degli sconti transazionali
		while (my @riga = $sth_estrai_benefici_T->fetchrow_array()) {
			$id_beneficio				=  $riga[0];
			$tipo_beneficio				=  $riga[1];
			$idpromo					=  $riga[2];
			$codice_promo				=  $riga[3];
			$valore_coinvolto_totale	=  $riga[4];
			$valore_sconto_totale		=  $riga[5];
			$punti_totali				=  $riga[6];
			
			if ($codice_promo ne '0061' and $codice_promo ne '0481') {	
				$totale_scontrino_progressivo+=$valore_sconto_totale;
				$somma_sconti_transazionali+=$valore_sconto_totale;
				$somma_punti_transazionali+=$punti_totali;
	
				my $promolistitem = $tipo_beneficio.$codice_promo.$idpromo.'+000000';
	
				# devo spalmare lo sconto e i punti sulle vendite
				# bisogna considerare il campo no_promo_flag per capire come considerare le vendite
	
				if(!$sth_spalma_sconti_transazionali->execute($promolistitem,$valore_sconto_totale,$punti_totali,$valore_sconto_totale,	$base_sconti_transazionali,
					$punti_totali,$base_punti_transazionali,$id_inizio_transazione,$id_fine_transazione,$transazione_corrente)) {
					print "errore in fase di esecuzione dello statement sth_spalma_sconti_transazionali\n";
				} else {
					# bisogna allocare sulla vendita maggiore il residuo dei punti e dello sconto transazionali non divisibile e quindi non spalmato
					if(!$sth_tot_sconto_ventilato->execute(	$id_inizio_transazione,$id_fine_transazione,$transazione_corrente)){
						print "errore in fase di esecuzione dello statement sth_tot_sconto_ventilato\n";
					} else {
						my $totale_punti_spalmati = 0;
						my $punti_residui = 0;
						my $totale_sconto_spalmato = 0;
						my $sconto_residuo = 0;
						# vado ad allocare sulla (prima) vendita con valore netto massimo i punti e lo sconto residuali
						my @record;
						while (my @record = $sth_tot_sconto_ventilato->fetchrow_array()) {
							$totale_sconto_spalmato		= $record[0];
							$totale_punti_spalmati		= $record[1];
	
							$punti_residui = $somma_punti_transazionali - $totale_punti_spalmati;
							$sconto_residuo= $somma_sconti_transazionali - $totale_sconto_spalmato;
							# print $log_file_handler "$id_corrente, $transazione_corrente, $id_inizio_transazione, $id_fine_transazione,
							# $vendita_massima_punti_trans $punti_residui = $somma_punti_transazionali - $totale_punti_spalmati\n";
	
							if($sconto_residuo) {
								if(!$sth_alloca_residuo_sconto->execute($sconto_residuo,$id_inizio_transazione,$id_fine_transazione,$vendita_massima_sconto_trans)) {
									print "errore in fase di esecuzione dello statement sth_alloca_residuo_sconto\n";
								} else {
									# sconto residuo spalmati
								}
							} else {
								# nessuno sconto residuo da spalmare
							}
	
							if($punti_residui) {
								if (!$sth_alloca_residuo_punti->execute($punti_residui,$id_inizio_transazione,$id_fine_transazione,$vendita_massima_punti_trans)) {
									print "errore in fase di esecuzione dello statement sth_alloca_residuo_punti\n";
								} else {
									# punti residui spalmati
								}
							} else  {
								# nessun punto residuo da spalmare
							}
						}
						# print "Spalmato sconto di $valore_sconto_totale e punti $punti_totali su transazione $transazione_corrente ($sconto_residuo, $punti_residui)\n";
					}
				}
			} elsif ($codice_promo eq '0061') {
				$totale_scontrino_progressivo+=$valore_sconto_totale;
				$somma_sconti_transazionali_0061+=$valore_sconto_totale;
	
				my $promolistitem = $tipo_beneficio.$codice_promo.$idpromo.'+000000';
	
				# devo spalmare lo sconto e i punti sulle vendite
				# bisogna considerare il campo no_promo_flag per capire come considerare le vendite
		
				if(!$sth_spalma_sconti_transazionali_0061->execute($promolistitem,$valore_sconto_totale,$valore_sconto_totale,	$base_sconti_transazionali,
					$id_inizio_transazione,$id_fine_transazione,$transazione_corrente)) {
					print "errore in fase di esecuzione dello statement sth_spalma_sconti_transazionali\n";
				} else {
					# bisogna allocare sulla vendita maggiore il residuo dei punti e dello sconto transazionali non divisibile e quindi non spalmato
					if(!$sth_tot_sconto_ventilato_0061->execute($id_inizio_transazione,$id_fine_transazione,$transazione_corrente)) {
						print "errore in fase di esecuzione dello statement sth_tot_sconto_ventilato\n";
					} else {
						my $totale_sconto_spalmato = 0;
						my $sconto_residuo = 0;
						# vado ad allocare sulla (prima) vendita con valore netto massimo i punti e lo sconto residuali
						my @record;
						while (my @record = $sth_tot_sconto_ventilato_0061->fetchrow_array()) {
							$totale_sconto_spalmato		= $record[0];
	
							$sconto_residuo= $somma_sconti_transazionali_0061 - $totale_sconto_spalmato;
							# print $log_file_handler "$id_corrente, $transazione_corrente, $id_inizio_transazione, $id_fine_transazione,
							# $vendita_massima_punti_trans $punti_residui = $somma_punti_transazionali - $totale_punti_spalmati\n";
	
							if($sconto_residuo) {
								if(!$sth_alloca_residuo_sconto_0061->execute($sconto_residuo,$id_inizio_transazione,$id_fine_transazione,$vendita_massima_sconto_trans)) {
									print "errore in fase di esecuzione dello statement sth_alloca_residuo_sconto\n";
								} else {
									# sconto residuo spalmati
								}
							} else {
								# nessuno sconto residuo da spalmare
							}
						}
						# print "Spalmato sconto di $valore_sconto_totale e punti $punti_totali su transazione $transazione_corrente ($sconto_residuo, $punti_residui)\n";
					}
				}
			} elsif ($codice_promo eq '0481') {                
#                if ($promolistitem ne $tipo_beneficio.$codice_promo.substr($idpromo,0,4).'   +000000') {
#                    $promolistitem = $tipo_beneficio.$codice_promo.substr($idpromo,0,4).'   +000000';
#                    $somma_sconti_transazionali_0481 = 0;
#				} else {
#					#bisogna calcolare il totale di reparto
#					if($sth_totale_sconti_reparto_0481->execute($id_inizio_transazione,$id_fine_transazione,$transazione_corrente,substr($idpromo,0,4))) {
#						while (my @record = $sth_tot_sconto_ventilato_0481->fetchrow_array()) {
#							$somma_sconti_transazionali_0481 = $record[0];
#						}
#					}
#				}

				if($sth_totale_sconti_reparto_0481->execute($id_inizio_transazione,$id_fine_transazione,$transazione_corrente,substr($idpromo,0,4))) {
					while (my @record = $sth_totale_sconti_reparto_0481->fetchrow_array()) {
						$somma_sconti_transazionali_0481 = $record[0];
					}
				}
				
                $totale_scontrino_progressivo+=$valore_sconto_totale;
				my $base_sconti_reparto = 0;
				my $vendita_massima_sconto_rep = 0;
				my $idx = firstidx { $_ == substr($idpromo,0,4) } @ar_reparto;
				if ($idx >= 0) {
					$base_sconti_reparto = $ar_base_sconti_reparto[$idx];
					$vendita_massima_sconto_rep=$ar_vendita_massima_sconto_reparto[$idx];
				} else {
					print "errore: reparto non trovato ripartizione sconti 0481 (ID da $id_inizio_transazione a $id_fine_transazione :$transazione_corrente)\n";
				}
				
				$somma_sconti_transazionali_0481 +=$valore_sconto_totale;

	
				# devo spalmare lo sconto e i punti sulle vendite
				# bisogna considerare il campo no_promo_flag per capire come considerare le vendite
		
				if(!$sth_spalma_sconti_reparto_0481->execute($promolistitem,$valore_sconto_totale,$valore_sconto_totale,$base_sconti_reparto,
					$id_inizio_transazione,$id_fine_transazione,$transazione_corrente,substr($idpromo,0,4))) {
					print "errore in fase di esecuzione dello statement sth_spalma_sconti_transazionali\n";
				} else {
					# bisogna allocare sulla vendita maggiore il residuo dei punti e dello sconto transazionali non divisibile e quindi non spalmato
					if(!$sth_tot_sconto_ventilato_0481->execute($id_inizio_transazione,$id_fine_transazione,$transazione_corrente, substr($idpromo,0,4))) {
						print "errore in fase di esecuzione dello statement sth_tot_sconto_ventilato\n";
					} else {
						my $totale_sconto_spalmato = 0;
						my $sconto_residuo = 0;
						# vado ad allocare sulla (prima) vendita con valore netto massimo i punti e lo sconto residuali
						my @record;
						while (my @record = $sth_tot_sconto_ventilato_0481->fetchrow_array()) {
							$totale_sconto_spalmato		= $record[0];
	
							$sconto_residuo= $somma_sconti_transazionali_0481 - $totale_sconto_spalmato;
							# print $log_file_handler "$id_corrente, $transazione_corrente, $id_inizio_transazione, $id_fine_transazione,
							# $vendita_massima_punti_trans $punti_residui = $somma_punti_transazionali - $totale_punti_spalmati\n";
	
							if($sconto_residuo) {
								if(!$sth_alloca_residuo_sconto_0481->execute($sconto_residuo,$id_inizio_transazione,$id_fine_transazione,$vendita_massima_sconto_rep, substr($idpromo,0,4))) {
									print "errore in fase di esecuzione dello statement sth_alloca_residuo_sconto\n";
								} else {
									#print "$sconto_residuo\n";
                                    #sconto residuo spalmati
								}
							} else {
								# nessuno sconto residuo da spalmare
							}
						}
						# print "Spalmato sconto di $valore_sconto_totale e punti $punti_totali su transazione $transazione_corrente ($sconto_residuo, $punti_residui)\n";
					}
				}
			}
		}
	}

	# gestione reparti
	my $somma_reparti = 0;
    # foreach my $reparto (keys %totale_reparto) {
        # my $tot_rep = $totale_reparto{$reparto};
        # $somma_reparti+=$tot_rep;
        # if (!$sth_aggiorna_reparto->execute($tot_rep,
                                            # $id_inizio_transazione,
                                            # $id_fine_transazione,
                                            # $reparto)) {
            # print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
            # return 0;
        # }
    # }

	&TestQuadraturaTransazione($totale);

    # valorizzo alcune variabili globali
    $tr_aperta              		= 0;
    $tr_tessera             		= '';
    $totale_scontrino_progressivo   = 0;
    $pti_transazione        		= 0;
    $quota_nonpagata        		= 0;
    $valore_lordo           		= 0;
    $unita_vendute  				= 0;
    $qta_venduta            		= 0;
    $deprezzato             		= 0;
    $bollone						= 0;
    $tipo_pagamento         		= '';
    $buono_sconto           		= 0;
    $pti_articolo           		= 0;
    $pti_target             		= 0;
    $codice_negozio_itm     		= 'N.D.';
    $id_inizio_transazione  		= 0;
    $id_fine_transazione    		= 0;
    $tr_tipo                		= 0;
    undef(@transazione);
    undef(%totale_reparto);

    $m_transazionale  			= 0;
    $m_semplice       			= 0;
    $tipo_i           			= '';
    $no_promo_flag    			= 0;
    $cassiere         			= '';
    $ora_transazione  			= '';
}

sub Update_Vendita() {
    if (!$update_vendita) {
        return 1;
    }

    if (!$sth_aggiorna_vendita->execute($unita_vendute,
					$qta_venduta,
                                        $valore_lordo,
                                        $valore_lordo,
                                        $tipo_i,
                                        $pti_articolo,
                                        $pti_target,
                                        $quota_nonpagata,
                                        $deprezzato,
					$no_promo_flag,
                                        $buono_sconto,
                                        $id_ultima_vendita,
                                        )) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }

    $update_vendita = 0;
    return 1;
}

# recupero su db il codice negozio Italmark ufficiale sulla base del codice letto nell'header del record
sub Transcode_Negozio() {
    my($header_neg_code, @other) = @_;

    #eseguo la query
    $sth_negozio->execute($header_neg_code);

    # scorro il result set
    my @dati;
    while (my @riga = $sth_negozio->fetchrow_array()) {
        @dati = @riga;
    }


    if ($#dati < 0) {
        #print "Non esiste un codice negozio associato a $header_neg_code\n";
        return $header_neg_code;
    }
    else {
        if ($dati[0]) {
            my $itm_code = $dati[0];
            #print "Trovata associazione $header_neg_code --> $itm_code\n";
			if ($itm_code ne $last_neg) {
#				print "Elaborazione Negozio $itm_code su datacollect_rich\n";
				$last_neg = $itm_code;
			}
            return $itm_code;
        }
    }
}

# recupero su db il codice articolo Italmark associato al barcode letto
sub Transcode_Barcode() {
    my($ean, $barcode_itm, $code1, $code2, $code3, $reparto, $valore, @other) = @_;
    my $articolo = "       ";

    $$barcode_itm = $ean;
    $$barcode_itm =~s/\s//ig;

    if ($$barcode_itm ne "") {
		# qui manipolo il barcode per portarlo nella forma ITM utile per la ricerca nell'archvio codici a barre
		# se si tratta di barcode "automatici" determino subito il codice articolo senza acedere al database

		# borsina
        if($$barcode_itm eq 1){
            $$barcode_itm = 17;
        }
        # articolo a peso variabile
        elsif ($$barcode_itm >= 2000000000000 and $$barcode_itm <= 2999999999999) {
            # deprezzati a peso variabile
            if($$barcode_itm >= 2000000000000 and $$barcode_itm <= 2199999999999 and substr($$barcode_itm,6,1) eq 9) {
                $$barcode_itm = substr($$barcode_itm,0,6)."0";
				$deprezzato = 1;
            }
            # vendite ingrosso
            elsif($$barcode_itm >= 2000000000000 and $$barcode_itm <= 2199999999999 and substr($$barcode_itm,6,1) eq 8) {
                $$barcode_itm = substr($$barcode_itm,0,6)."0";
				$deprezzato = 1;
            }

            else {
                $$barcode_itm = substr($$barcode_itm,0,7);
            }
        }
        # deprezzati cad
        elsif($$barcode_itm >= 9999900000000 and $$barcode_itm <= 9999999999999) {
            $articolo = substr($$barcode_itm,5,7);
			$deprezzato = 1;
            return $articolo;
        }
        # vendite negative/storni
        elsif($$barcode_itm >= 9977700000000 and $$barcode_itm <= 9977799999999) {
            $articolo = substr($$barcode_itm,5,7);
            return $articolo;
        }
        # sconti su articoli
        elsif($$barcode_itm >= 9955500000000 and $$barcode_itm <= 9955599999999) {
            $articolo = &ArticoloReparto($reparto);
            return $articolo;
        }
		elsif ($$barcode_itm >= 9980110000000 and $$barcode_itm <= 9980110009999){
			
			$bollone = 1;
			if ($$barcode_itm eq 9980110009999) {
				$bollone = 2;
			}
			
			# in caso di storno/reso/annullo di deprezzato, il record C precede il record S del bollone
			if($code2 =~ /(4|7)/){
				# devo cercare un record C aperto precedente
				if(!$sth_select_max_beneficio_semplice->execute($id_inizio_transazione)){
					print "errore nell'esecuzione dello statement sth_select_max_beneficio_semplice\n";
				}
				else{

					if (!$sth_select_max_beneficio_semplice->rows) {
						print "Errore: nessun beneficio di tipo C aperto trovato a fronte dello storno del bollone";
					}
					my $last_id = 0;
					while (my @riga = $sth_select_max_beneficio_semplice->fetchrow_array()) {
						$last_id =  $riga[0];
						if(!$sth_update_max_beneficio_semplice->execute($id_corrente, $last_id)){
							print "errore nell'esecuzione dello statement sth_update_max_beneficio_semplice\n";
						}
						else{
							$bollone = 0;
							#Marco G.
							#in realtˆ in caso di storno bollone nessun C precede le vendita e, sopratutto,
							#non ci sono record C precedenti ancora aperti. E'sbagliato lo scontrino perci˜ non posso
							#fare nulla.
							$m_semplice--;

						}
					}
				}
			}
		}
    }
    else {
        # barcode vuoto: è una vendita a tasto
        $articolo = &ArticoloReparto($reparto);
        return $articolo;
    }

    #eseguo la query che recupera il codice articolo dall'archivio codici a barre
    $sth_barcode->execute($$barcode_itm);

    # scorro il result set
    my @dati;
    while (my @riga = $sth_barcode->fetchrow_array()) {
        @dati = @riga;
    }

    if ($#dati < 0) {
        #print $log_file_handler "Non esiste un codice articolo associato a $ean ($$barcode_itm)\n";
        &UpdateErroriBarcode($$barcode_itm, $codice_negozio_itm, $reparto, 0, $valore);
        $articolo = &ArticoloReparto($reparto);
        #print $log_file_handler "$$barcode_itm --> $articolo\n";
    }
    else {
        if ($dati[0]) {
            $articolo = $dati[0];
            #print $log_file_handler "Trovata associazione $$barcode_itm --> $articolo\n";
            &UpdateErroriBarcode($$barcode_itm, $codice_negozio_itm, $reparto, 1, $valore);
        }
    }

    return $articolo;
}

sub ArticoloReparto() {
    my($reparto)=@_;

    my %articoli_reparto = (
    '0001'    => '9910000',
    '0002'    => '9920008',
    '0003'    => '9940004',
    '0004'    => '9950001',
    '0005'    => '9960009',
    '0006'    => '9970007',
    '0007'    => '9980005',
    '0008'    => '9990003',
    '0009'    => '9930006',
    '0100'    => '9910000',
    '0111'    => '9910000',
    '0112'    => '9910000',
    '0113'    => '9910000',
    '0114'    => '9910000',
    '0120'    => '9910000',
     '0125'    => '9910000',
     '0130'    => '9910000',
     '0140'    => '9910000',
     '0145'    => '9910000',
     '0150'    => '9910000',
     '0155'    => '9910000',
     '0170'    => '9910000',
     '0171'    => '9910000',
     '0175'    => '9910000',
     '0177'    => '9910000',
     '0178'    => '9910000',
     '0180'    => '9910000',
     '0190'    => '9910000',
     '0191'    => '9910000',
     '0200'    => '9920008',
     '0210'    => '9920008',
     '0250'    => '9920008',
     '0280'    => '9930006',
     '0300'    => '9940004',
     '0310'    => '9940004',
     '0350'    => '9940004',
     '0380'    => '9940004',
     '0400'    => '9950001',
     '0405'    => '9960009',
     '0407'    => '9980005',
     '0420'    => '9950001',
     '0450'    => '9950001'
    );

    my $articolo = $articoli_reparto{$reparto};
    if(! $articolo) {
        $articolo = '       ';
		print "Reparto non pevisto: $reparto, ID=$id_corrente\n";
    }

    return $articolo;
}

sub UpdateErroriBarcode() {
    my($barcode, $codice_negozio, $reparto, $success, $valore, @other)=@_;

    if($success) {
        $sth_delete_barcode->execute($barcode);
    }
    else {
        my $ultima_rilevazione = $data_riferimento.$codice_negozio.$record{'cassa'}.$tr_corrente;
        $sth_insert_barcode->execute($barcode, $reparto, $ultima_rilevazione, $valore, $ultima_rilevazione, $ultima_rilevazione, $valore);
    }
}

sub AddCodiceArticolo() {
    my($PLU, $codice_articolo, $barcode_itm, @other) = @_;
    if (!$sth_abbina_articolo->execute($PLU, $codice_articolo, $barcode_itm, $id_corrente)) {
        print "Errore durante l'esecuzione di una query su db (-$codice_articolo-, -$id_corrente-)! " .$dbh->errstr."\n";
    }
}

sub G_Record() {
    my ($linea, @other) = @_;

    my $code1           ;
    my $code2           ;
    my $code3           ;
    my $reparto         ;
    my $type            ;
    my $complex         ;
    my $plu_number      ;
    my $action_code     ;
    my $ammontare_punti ;
    my $related_amount  ;

    if ($record{'sottorecord'} =~ /^((0|1|2|3|4){1})((0|1|2|3|5|6|7|8|9){1})((1|2){1}):(\d{4}):((P|D|\s){1})((0|1){1}):(.{13}):(\d{2})((\+|\-)\d{5})((\+|\-)\d{9})$/) {

        $code1              = $1;
        $code2              = $3;
        $code3              = $5;
        $reparto            = $7;
        $type               = $8;
        $complex            = $10;
        $plu_number         = $12;
        $action_code        = $13;
        $ammontare_punti    = $14;
        $related_amount     = $16;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }


	if ($tr_tipo ne 1) {
		return 1;
	}


    # se beneficio transazionale deve essere preceduto da record m con code3=3
    if($type eq ' '){# Transaction point
        if(!$m_transazionale){
            print "Record G con code2=2 ma senza record m con code3=3 (ID=$id_corrente)\n";
        }
        else{
            $m_transazionale--; # "brucio" il record m transazionale
			# sconto transazionale preceduto da record m (a fronte del quale è stata fatta la insert)
			# bisogna aggiornare (un solo) beneficio con tipo_insert = m in stato 'I' e chiuderlo
			# considerare solo alcuni tipo promo?

			#eseguo la query di ricerca di benefici con le caratteristiche volute
			$sth_select_beneficio_trans->execute($id_inizio_transazione,'0034');

			my $benefici_trovati = $sth_select_beneficio_trans->rows;
			if(! $benefici_trovati ){
				print "Nessun beneficio trovato con le caratteristiche volute per record G transazionale ($id_corrente)\n";
			}
			elsif ($benefici_trovati > 1){
				print "Trovati $benefici_trovati benefici (ma atteso 1) con le caratteristiche volute per record G transazionale ($id_corrente)\n";
			}
			else {
				# scorro il result set
				my @dati;
				while (my @riga = $sth_select_beneficio_trans->fetchrow_array()) {
					@dati = @riga;
				}
				my $beneficio_da_aggiornare = $dati[0];
				my $id_beneficio_insert = $dati[1];

				if (! $sth_update_beneficio_trans_G->execute($ammontare_punti, $related_amount, $id_corrente,$id_corrente,$beneficio_da_aggiornare)){
					print "Errore nell'esecuzione dello statement sth_update_beneficio_trans_G\n";
				}
				else {
					# print "Record G $id_corrente aggiorna beneficio $beneficio_da_aggiornare (id $id_beneficio_insert)\n";
				}
			}
        }
    }
    else {
		# record G seguito da record m
        $m_semplice++;

		my $valore_articolo;
		my $punti_articolo;
		my $qta_articolo;

		if($type eq 'P'){
			$valore_articolo = $related_amount;
			$punti_articolo	 = $ammontare_punti;
		}
		else{ # type = D
			$valore_articolo = 0;
			$punti_articolo  = 0;
		}

		# se ci sono dei record d precedenti fanno riferimento a questo G
		if($dettaglio_in_corso) {
			$dettaglio_in_corso = 0; # fine dei dettagli
			# faccio l'update dei record d
			# Extrapunti (promo 0505)
			# il record G fa riferimento ad un plu e ad un valore di una vendita (l'ultima) e non all'intero set che comporta lo sconto
			# assumo che il valore totale sia costruito sulla base dei record d perchè nel record G c'è un valore relativo solo alla vendita di un PLU
			# Spalmo i punti (parte intera) sulla base della quota di ogni plu sul valore coinvolto totale
#            print "update	ncr.benefici
#				set		id_update			= $id_corrente	,
#						tipo_beneficio		= 'G',
#						punti_articolo		= ifnull(truncate($ammontare_punti * (valore_coinvolto_articolo/$valore_coinvolto_totale_d),0),0),
#						punti_totali		= $ammontare_punti	,
#						valore_coinvolto_totale = $valore_coinvolto_totale_d
#				where	binary tipo_record_insert  = 'd'
#				and		stato 				= 'I'
#				and		tipo_beneficio		= ''
#				and		livello_beneficio	= 'P'
#				and		id_insert			> $id_inizio_transazione
#				and		id_insert			< $id_corrente\n";
			if(!$sth_update_beneficio_d_G->execute(	$id_corrente,
													$ammontare_punti,
													$valore_coinvolto_totale_d,
													$ammontare_punti,
													$valore_coinvolto_totale_d,
													$id_inizio_transazione,
													$id_corrente)){
				print "Errore nell'aggiornamento dei record d da record G\n";
			}

			# devo assegnare all'articolo con quota maggiore i punti non ancora spalamti (per via dei troncamenti)
			# coinvolgo solo i record d che sono stati aggiornati dal record G corrente (idupdate = idcorrente)
			# biosgna recuperare l'articolo con quota maggiore e la differenza tra punti totali e punti spalmati
			if(!$sth_select_d_punti_residui_G->execute($id_corrente)){
				print "Errore nell'esecuzione dello statement sth_select_d_punti_residui_G\n";
			}
			else {
				# scorro il result set
				my @dati;
				while (my @riga = $sth_select_d_punti_residui_G->fetchrow_array()) {
					@dati = @riga;
				}
				#print "$id_corrente\n";
				my $quota_valore_massimo = $dati[0];
				my $punti_da_allocare = $ammontare_punti - $dati[1];
				if($punti_da_allocare){
					# vado ad attribuire i punti non ancora distribuiti al record d con quota massima
					if (! $sth_update_d_punti_residui_G ->execute($punti_da_allocare, $id_corrente, $quota_valore_massimo)){
						print "Errore nell'esecuzione dello statement sth_update_d_punti_residui_G\n";
					}
					else {
						#print "Aggiunti punti $punti_da_allocare ($ammontare_punti - $dati[1]) al record d con valore $quota_valore_massimo\n";
					}
				}
			}

			$valore_coinvolto_totale_d = 0;

		}
		else {
			# inserisco un nuovo beneficio
			# bisogna creare un nuovo record nella tabella dei benefici
			if (!$sth_insert_beneficio->execute(
							'G'                         ,       # tipo_record_insert
							'G'                         ,       # tipo_beneficio
							$type                       ,       # livello_beneficio (P o D)
							$idpromo                    ,       # idpromo
							''                          ,       # codice_promo
							$plu_number                 ,       # plu
							$reparto                    ,       # reparto
							$transazione_corrente       ,       # transazione
							'I'                         ,       # stato
							0                           ,       # qta_coinvolta_articolo
							$valore_articolo            ,       # valore_coinvolto_articolo
							$related_amount             ,       # valore_coinvolto_totale
							0                           ,       # valore_sconto_totale
							0                           ,       # valore_sconto_articolo
							$ammontare_punti            ,       # punti_totali
							$punti_articolo             ,       # punti_articolo
							0	                        ,       # id_vendita_abbinata
							$id_corrente				,
							0							,
							0
							)) {
				print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
				return 0;
			}
			else {
				#print "Inserito beneficio G con stato I\n";
			}
		}
    }

    return 1;
}

# Box/Set richiamo transazione
sub B_Record() {
    my ($linea, @other) = @_;

    my $code1       ;
    my $code2       ;
    my $code3       ;
    my $cassiere    ;
    my $numeroid    ;
    my $action_code ;
    my $contatore   ;
    my $ammontare   ;

    if ($record{'sottorecord'} =~ /^(\d{1})((0|5|6){1})((0|1){1}):(\d{4}):((\d|\s){16}):(\d{2})((\+|\-|\d){6})((\d|\+|\-){10})$/) {
        $code1          = $1;
        $code2          = $2;
        $code3          = $4;
        $cassiere       = $6;
        $numeroid       = $7;
        $action_code    = $9;
        $contatore      = $10;
        $ammontare      = $12;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

# Sconti / Svalorizzazioni automatici
sub C_Record() {
    my ($linea, @other) = @_;

    my $code1     ;
    my $code2     ;
    my $code3     ;
    my $cds       ;
    my $reparto   ;
    my $plu  ;
    my $fisso     ;
    my $contatore ;

    my $tipo;
    my $complex     ;
    my $qta_intera  ;
    my $decimal     ;
    my $qta_decimale;
    my $sconto      ;

    # C Record x Asar
    if ($record{'sottorecord'} =~ /^(\d{1})(4{1})((2|3|5|6){1}):(\d{4}):((P|D){1})((0|1){1}):(.{13})((\+|\-)\d{4})((\.|\d){1})(\d{3})((\+|\-)\d{9})$/){
        $code1        	= $1;
        $code2        	= $2;
        $code3        	= $3;
        $reparto      	= $5;
        $tipo         	= $6; # P(plu) D(departement)
        $complex      	= $8;
        $plu     	= $10;
        $qta_intera   	= $11;
        $decimal      	= $13;
        $qta_decimale 	= $15;
        $sconto       	= $16;
    }
    else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

	if ($tr_tipo ne 1) {
		return 1;
	}

	my $qta = 0;
	# il contenuto del campo $decimale è '.' oppure 0
    if($decimal eq '.') {
        # quantità decimale
        $qta = $qta_intera.$decimal.$qta_decimale;
    }
    else {
        $qta = $qta_intera;
    }

	$idpromo			= '';
	my $codice_promo 	= '';
	my $stato           = 'I';
	my $idclose			= 0;

    # se lo sconto  uno sconto set (143) potrebbe essere un deprezzamento
	# nel caso di deprezzamento non c'è il record m
	if(($code3 eq 3) && ($bollone)){
		
		# non incremento il contatore m semplice
		
		$idpromo		= '1111111';
		if ($bollone == 2) {
			$idpromo		= '2222222';
		}
		$bollone = 0; # brucio il bollone
		
		$codice_promo		= '0492';
		$stato			= 'C';
		$idclose		= $id_corrente;
	}
	else {
		$m_semplice++;
	}

	my $sconto_articolo=0;
	my $qta_articolo=0;
	if($tipo eq 'P'){ # beneficio articolo
		$sconto_articolo = $sconto;
		$qta_articolo = $qta;
	}
	else { # beneficio reparto
	}

	# inserisco un nuovo beneficio
	# bisogna creare un nuovo record nella tabella dei benefici
    if (!$sth_insert_beneficio->execute(
                    'C'                         ,       # tipo_record_insert
                    'C'                         ,       # tipo_beneficio
                    $tipo                       ,       # livello_beneficio (P o D)
                    $idpromo                    ,       # idpromo
                    $codice_promo               ,       # codice_promo
                    $plu                        ,       # plu
                    $reparto                    ,       # reparto
                    $transazione_corrente       ,       # transazione
                    $stato                      ,       # stato
                    $qta_articolo               ,       # qta_coinvolta_articolo
                    0                           ,       # valore_coinvolto_articolo
                    0                           ,       # valore_coinvolto_totale
                    $sconto                     ,       # valore_sconto_totale
					$sconto_articolo            ,       # valore_sconto_articolo
					0                           ,       # punti_totali
					0                           ,       # punti_articolo
					0	                        ,       # id_vendita_abbinata
					$id_corrente				,
					0							,
					$idclose
					)) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    else {
        #print "Inserito beneficio C con stato $stato\n";
    }

    return 1;
}

sub D_Record() {
    my ($linea, @other) = @_;

    my $code1        ;
    my $code2        ;
    my $code3        ;
    my $dep_number   ;
    my $filler       ;
    my $complex      ;
    my $detail       ;
    my $filler2      ;
    my $action_code  ;
    my $qta          ;
    my $valore       ;


    if ($record{'sottorecord'} =~ /^(\d{1})((\d){1})((\d){1}):(\d{4}):(\s)(0|1):(0|1):(\s{11}):(\d{2})((\+|\-)\d{5})((\+|\-)\d{9})$/) {

        $code1        = $1;
        $code2        = $2;
        $code3        = $4;
        $dep_number   = $6;
        $filler       = $7;
        $complex      = $8;
        $detail       = $9;
        $filler2      = $10;
        $action_code  = $11;
        $qta          = $12;
        $valore       = $14;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }


	if ($tr_tipo ne 1) {
		return 1;
	}


    if($code2==9) { # automatic discount
        if ($code3 == 7 || $code3 == 8){ # automatic transaction discount (6=reparto)
			if ($detail) {
				print "Record D con valore di dettaglio non previsto, $linea\n";
			}

            if(!$m_transazionale){
#                print "Record D transazionale ma senza record m con code3=3 (ID=$id_corrente)\n";
				$m_transazionale++;

				# inserisco un nuovo beneficio
				# bisogna creare un nuovo record nella tabella dei benefici
				if (!$sth_insert_beneficio->execute(
								'D'                         ,       # tipo_record_insert
								'D'                         ,       # tipo_beneficio
								'T'                         ,       # livello_beneficio (P o D)
								''                          ,       # idpromo
								''			                ,       # codice_promo
								''                          ,       # plu
								''                          ,       # reparto
								$transazione_corrente       ,       # transazione
								'I'                         ,       # stato
								0			                ,       # qta_coinvolta_articolo
								0                           ,       # valore_coinvolto_articolo
								0                           ,       # valore_coinvolto_totale
								$valore                     ,       # valore_sconto_totale
								0                           ,       # valore_sconto_articolo
								0                           ,       # punti_totali
								0                           ,       # punti_articolo
								0	                        ,       # id_vendita_abbinata
								$id_corrente				,
								0							,
								0
								)) {
					print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
					return 0;
				}
				else {
					#print "Inserito beneficio D con stato I\n";
				}
            }
            else{
                $m_transazionale--; # "brucio" il record m transazionale
				# sconto transazionale preceduto da record m (a fronte del quale è stata fatta la insert
				# bisogna aggiornare (un solo) beneficio con tipo_insert = m in stato 'I'e chiuderlo
				# considerare solo alcuni tipo promo?

				#eseguo la query di ricerca di benefici con le caratteristiche volute
				$sth_select_beneficio_trans->execute($id_inizio_transazione, '0061');

				my $benefici_trovati = $sth_select_beneficio_trans->rows;
				if(! $benefici_trovati ){
					print "Nessun beneficio trovato con le caratteristiche volute per record D transazionale ($id_corrente)\n";
				}
				elsif ($benefici_trovati > 1){
					print "Trovati $benefici_trovati benefici (ma atteso 1) con le caratteristiche volute per record D transazionale ($id_corrente)\n";
				}
				else {
					# scorro il result set
					my @dati;
					while (my @riga = $sth_select_beneficio_trans->fetchrow_array()) {
						@dati = @riga;
					}
					my $beneficio_da_aggiornare = $dati[0];
					my $id_beneficio_insert = $dati[1];

					#print "$linea\n";
					if (! $sth_update_beneficio_trans_D->execute($valore, $id_corrente,$id_corrente, $beneficio_da_aggiornare)){
						print "Errore nell'esecuzione dello statement sth_update_beneficio_trans_D\n";
					}
					else {
						# print "Record D $id_corrente aggiorna beneficio $beneficio_da_aggiornare (id $id_beneficio_insert)\n";
					}
				}
            }
        }
        elsif ($code3 == 6){ # set
			# mi aspetto di trovare il campo datail valorizzato a 1
			if(!$detail){
				print "Record D di tipo set ma che non prevede d\n";
			}
			else{
				# record D prevede d precedenti
				if(!$dettaglio_in_corso){
					print "Record D che prevede d precedenti ma non trovati\n"
				}
				else{
					$dettaglio_in_corso = 0;
					$m_semplice++;

					# faccio l'update dei record d
					# assumo che il valore totale sia costruito sulla base dei record d
					# Spalmo lo sconto sulla base della quota di ogni plu sul valore coinvolto totale

					if(!$sth_update_beneficio_d_D->execute(	$id_corrente,
															$valore,
															$valore_coinvolto_totale_d,
															$valore,
															$valore_coinvolto_totale_d,
															$id_inizio_transazione,
															$id_corrente)){
						print "Errore nello statement sth_update_beneficio_d_D\n";
					}

					# devo assegnare all'articolo con quota maggiore lo socnto non ancora spalmato (per via dei troncamenti)
					# coinvolgo solo i record d che sono stati aggiornati dal record D corrente (idupdate = idcorrente)
					# biosgna recuperare l'articolo con quota maggiore e la differenza tra sconto totale e sconto spalmato
					if(!$sth_select_d_sconto_residuo_D->execute($id_corrente)){
						print "Errore nell'esecuzione dello statement sth_select_d_sconto_residuo_D\n";
					}
					else {
						# scorro il result set
						my @dati;
						while (my @riga = $sth_select_d_sconto_residuo_D->fetchrow_array()) {
							@dati = @riga;
						}
						my $quota_valore_massimo = $dati[0];
						my $sconto_da_allocare = $valore - $dati[1];
						if($sconto_da_allocare){
							# vado ad attribuire lo socnto non ancora distribuito al record d con quota massima
							if (! $sth_update_d_sconto_residuo_D ->execute($sconto_da_allocare, $id_corrente, $quota_valore_massimo)){
								print "Errore nell'esecuzione dello statement sth_update_d_sconto_residuo_D\n";
							}
							else {
								#print "Aggiunto sconto $sconto_da_allocare ($valore - $dati[1]) al record d con valore $quota_valore_massimo\n";
							}
						}
					}

					$valore_coinvolto_totale_d = 0;
				}
			}
        }
        else{
            print "Record d con code3=$code3 non atteso\n";
        }
    }
    else {
        print "Record d con code2=$code2 non atteso\n";
    }
    return 1;
}


# dettaglio sconti
sub d_Record() {
    my ($linea, @other) = @_;

    my $code1        ;
    my $code2        ;
    my $code3        ;
    my $dep_number   ;
    my $type         ;
    my $complex      ;
    my $plu          ;
    my $qta          ;
    my $code4        ;
    my $decimal_qta  ;
    my $segno_valore ;
    my $valore       ;


    if ($record{'sottorecord'} =~ /^(\d{1})((0){1})((0){1}):(\d{4}):(P|D)(0|1):((\d|\s){13})((\+|\-)\d{4})(0|\.)(\d{3})((\+|\-|\*))(\d{9})$/) {

        $code1          = $1;
        $code2          = $2;
        $code3          = $4;
        $dep_number     = $6;
        $type           = $7;
        $complex        = $8;
        $plu            = $9;
        $qta            = $11;
        $code4          = $13;
        $decimal_qta    = $14;
        $segno_valore   = $15;
        $valore         = $17;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

	if ($tr_tipo ne 1) {
		return 1;
	}


    # il contenuto del campo $code4 è '.' oppure 0
    if($code4 eq '.') {
        # quantità decimale
        $qta = $qta.$decimal_qta;
    }
    else {
        # $qta = $qta
    }

    if($segno_valore eq '*'){
        $valore=$valore*$qta;
    }
    else {
        # $valore=$valore
    }

    if( ! $dettaglio_in_corso) {
        $dettaglio_in_corso = 1;
        $valore_coinvolto_totale_d = 0;
    }
    $valore_coinvolto_totale_d+=$valore;

	my $qta_articolo=0;
	my $valore_articolo=0;
	if($type eq 'P'){ # beneficio articolo
		$qta_articolo = $qta;
		$valore_articolo = $valore;
	}
	else { # beneficio reparto
	}

	# inserisco un nuovo beneficio
	# bisogna creare un nuovo record nella tabella dei benefici
    if (!$sth_insert_beneficio->execute(
                    'd'                         ,       # tipo_record_insert
                    ''                          ,       # tipo_beneficio
                    $type                       ,       # livello_beneficio (P o D)
                    ''                          ,       # idpromo
                    ''                          ,       # codice_promo
                    $plu                        ,       # plu
                    $dep_number                 ,       # reparto
                    $transazione_corrente       ,       # transazione
                    'I'                         ,       # stato
                    $qta_articolo               ,       # qta_coinvolta_articolo
                    $valore_articolo            ,       # valore_coinvolto_articolo
                    0                           ,       # valore_coinvolto_totale
                    0                           ,       # valore_sconto_totale
					0                           ,       # valore_sconto_articolo
					0                           ,       # punti_totali
					0                           ,       # punti_articolo
					0	                        ,       # id_vendita_abbinata
					$id_corrente				,
					0							,
					0
					)) {
        print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
        return 0;
    }
    else {
        #print "Inserito beneficio d con stato I\n";
    }

    return 1;
}

# fine transazione
# fine transazione
sub F_Record() {
    my ($linea, @other) = @_;

    my $code1              ;
    my $code2              ;
    my $code3              ;
    my $codice_cassiere    ;
    my $info_aggiuntive    ;
    my $non_utilizzato     ;
    my $contatore_articoli ;
    my $totale  ;

    if ($record{'sottorecord'} =~ /^(\d{1})((0|1|5|6){1})((0|2|4){1}):((\d|\*){4}):(.{16}):(.{2})(.{6})(.{10})$/) {
        $code1               	= $1;
        $code2               	= $2;
        $code3               	= $4;
        $codice_cassiere     	= $6;
        $info_aggiuntive     	= $8;
        $non_utilizzato      	= $9;
        $contatore_articoli  	= $10;
        $totale   		= $11; # questo campo rappresenta il totale scontrino
    } else {
	return 0;
    }

    if ($code1 =~ /0/) {    #Money trans
        #  arriva un record di chiusura senza apertura: non ne faccio nulla
        $tr_aperta     = 0;
        return 1;
    } elsif ($code1 =~ /9/) {    # Reset Mode
        #  arriva un record di chiusura senza apertura: non ne faccio nulla
        return 1;
    } elsif ($code1 =~ /1/) {    # Transizione di vendita
        # gestito
    } elsif ($code1 =~ /2/) {    # Transazione abortita
         # potrebbe arrivare un record F senza il suo H
        $tr_corrente   = $record{'num_transazione'};
        $tr_aperta     = 1;
    } elsif ($code1 =~ /8/) {    # Transazione sospesa
        # gestito
    } else {
        # non gestito
        return 0;
    }

    # qua solo se record gestito
    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    $tr_aperta     = 0;
    $tr_corrente   = $record{'num_transazione'};

    if (($tr_tipo eq 1) && ($code1 eq 1) ) {
        # questa condizione permette di individuare tutte e sole le transazioni che partecipano alla costruzione dell'incasso
        # inoltre, nel campo totale è presente il valore totale dello scontrino

        # aggiorno l'ultima vendita con il valore al netto di tutti gli sconti
        if($update_vendita > 0){
            &Update_Vendita();
        }

        # a questo punto non dovrei avere "aperti"  dettagli di sconti transazionali
        if($m_transazionale){
            print "m transazionale non bruciato da alcun sconto transazionale (ID record F = $id_corrente, Linea = $linea)\n";
        }

        # a questo punto non dovrei avere "aperti"  dettagli di sconti semplici
        if($m_semplice){
            print "m semplice non bruciato (manca record m ma beneficio aperto) (ID record F = $id_corrente, Linea = $linea)\n";
        }

        $quota_nonpagata	= 0;
        $valore_lordo   	= 0;
        $unita_vendute  	= 0;
        $qta_venduta    	= 0;
        $deprezzato     	= 0;
	$bollone		= 0;
        $m_transazionale	= 0;
        $m_semplice     	= 0;
        $buono_sconto   	= 0;
        $pti_articolo   	= 0;
        $pti_target     	= 0;
        $tipo_i 		= '';
	$no_promo_flag  	= 0;

	$id_fine_transazione 	= $id_corrente;

	$cassiere         	= $codice_cassiere;
	$ora_transazione  	= $record{'ora'};

	&Chiusura_Transazione($totale);

	$cassiere         	= '';
	$ora_transazione  	= '';
    }

    return 1;
}

# informazioni di dettaglio sconti
sub m_Record() {
    my ($linea, @other) = @_;

    my $code1;
    my $code2;
    my $code3;
    my $information;
	my $codice_promo='';
    my $idpromo='';

    if ($record{'sottorecord'} =~ /^((0|1|2|3|4|8){1})(\d{1})((0|1){1}):(.{40})$/) {
        $code1          = $1;
        $code2          = $3;
        $code3          = $4;
        $information    = $6;
    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }


    if ($tr_tipo ne 1) {
        return 1;
    }


    # il campo information  user definible
    # la nostra convenzione  m:101:  00:0034-8999807
    if($information =~ /\s\s00:(\d{4})(-|\s)((\d|\w|\s){7})/){
        $codice_promo = $1;
        $idpromo = $3;
    }
    else{
        print $log_file_handler "Record m con campo information non in linea con lo standard (ID=$id_corrente, information = $information)\n";
    }

    # il record m segue sempre il beneficio a meno che si tratti di beneficio transazionale, nel qual caso il record m anticipa il record di beneficio
    # per verificare se il beneficio è transazionale bisogna controllare il code3
    if($code3 == 0){    # beneficio semplice
                        # il record m segue il beneficio, quindi qui "brucio" il dettaglio aperto in occasione del beneficion C, G o D
        $m_semplice--;

		if ($codice_promo eq '0027' or $codice_promo eq '0024') { #pago con nimis ho la sequenza CGm, un record m che dettaglia due record di beneficio
			$m_semplice--;
		}
		
		if ($codice_promo eq '9927') { #dono con nimis ho la sequenza GGm, un record m che dettaglia due record di beneficio
			$m_semplice--;
		}

		#eseguo la query di ricerca di benefici con le caratteristiche volute
		$sth_select_beneficio_semplice->execute($id_inizio_transazione);
        
#        print "select	id,
#					id_insert,
#					tipo_record_insert
#				from	ncr.benefici
#				where 	stato <> 'C'
#				and	((binary tipo_record_insert = 'G' and tipo_beneficio = 'G')
#				or	(binary tipo_record_insert = 'C' and tipo_beneficio = 'C')
#				or	(binary tipo_record_insert = 'd'))
#				and	livello_beneficio in ('P','D')
#				and	id_insert >= $id_inizio_transazione\n";

		my $benefici_trovati = $sth_select_beneficio_semplice->rows;
		if(! $benefici_trovati ){
			print "Nessun beneficio trovato con le caratteristiche volute per record m semplice ($id_corrente, $linea)\n";
		}
		else { # sono stati trovati benefici aperti (da aggiornare)
			# scorro il result set
			my @dati;
			my $sequenza='';
			my @id_benefici_da_aggiornare;

			while (my @riga = $sth_select_beneficio_semplice->fetchrow_array()) {
				@dati = @riga;
				push(@id_benefici_da_aggiornare, $dati[0]);
				my $id_beneficio_insert = $dati[1];
				$sequenza.=$dati[2];
			}

			if(		($codice_promo eq '0027' && $sequenza =~ /^(CG)/)
			  ||	($codice_promo eq '0024' && $sequenza =~ /^(CG)/)
				||	($codice_promo eq '9927' && $sequenza =~ /^(GG)/)
				||	($codice_promo eq '0022' && $sequenza =~ /^(G)/)
				||	($codice_promo eq '0023' && $sequenza =~ /^(G)/)
				||	($codice_promo eq '0055' && $sequenza =~ /^(d+)/)
				||	($codice_promo eq '0057' && $sequenza =~ /^(C)/)
				||	($codice_promo eq '0493' && $sequenza =~ /^(C)/)
				||	($codice_promo eq '0504' && $sequenza =~ /^(d+)/)
				||	($codice_promo eq '0505' && $sequenza =~ /^(d+)/)
				) {
				my $num_update = length($1);

				#print "$codice_promo $sequenza $num_update\n";

				for (my $count = 0; $count < $num_update; $count++) {
					my $id_update = $id_benefici_da_aggiornare[$count];
                    
					if (!$sth_update_beneficio_semplice_m->execute($idpromo, $codice_promo, $id_corrente, $id_update)) {
						print "errore aggiornamento benefici, record m, $idpromo, $codice_promo, $id_corrente, $id_update\n";
					}
				}
			}
			else{
				print "trovata sequenza $sequenza con codice_promo $codice_promo (id $id_corrente, trans: $transazione_corrente)\n";
			}
		}
    }
    else{ # beneficio transazionale
        if($m_transazionale){
			# ci sono delle (rare) situazioni per cui un record di beneficio (D) transazionale precede il record m di dettaglio
			# in questio casi devo aggiornare il beneficio (inserito a fronte del record D)
			# Cerco il beneficio da aggiornare
			if(!$sth_select_beneficio_trans_mD->execute($id_inizio_transazione)){
				print "Errore nell'esecuzione dello statement $sth_select_beneficio_trans_mD\n";
			}
			else{
				if($sth_select_beneficio_trans_mD->rows) {
					my @dati;
					while (my @riga = $sth_select_beneficio_trans_mD->fetchrow_array()) {
						@dati = @riga;
					}
					my $beneficio_da_aggiornare = $dati[0];

					if (! $sth_update_beneficio_semplice_m->execute($idpromo, $codice_promo, $id_corrente, $beneficio_da_aggiornare)){
						print "Errore nell'esecuzione dello statement sth_update_beneficio_trans_G\n";
					}
					else {
						# print "Record D $id_corrente aggiorna beneficio $beneficio_da_aggiornare\n";
						$m_transazionale--;
					}
				}
				else{
					print "Nessun beneficio transazionale di tipo D precede il corrente record m ($id_corrente)\n";
				}
			}
		}
		else{
			#bisogna creare il beneficio
			$m_transazionale++;

			# inserisco un nuovo beneficio
			# bisogna creare un nuovo record nella tabella dei benefici
			if (!$sth_insert_beneficio->execute(
							'm'                         ,       # tipo_record_insert
							''                          ,       # tipo_beneficio
							'T'                         ,       # livello_beneficio (P o D)
							$idpromo                    ,       # idpromo
							$codice_promo               ,       # codice_promo
							''                          ,       # plu
							''                          ,       # reparto
							$transazione_corrente       ,       # transazione
							'I'                         ,       # stato
							0			                ,       # qta_coinvolta_articolo
							0                           ,       # valore_coinvolto_articolo
							0                           ,       # valore_coinvolto_totale
							0                           ,       # valore_sconto_totale
							0                           ,       # valore_sconto_articolo
							0                           ,       # punti_totali
							0                           ,       # punti_articolo
							0	                        ,       # id_vendita_abbinata
							$id_corrente				,
							0							,
							0
							)) {
				print "Errore durante l'esecuzione di una query su db! " .$dbh->errstr."\n";
				return 0;
			}
			else {
				#print "Inserito beneficio m con stato I\n";
			}
		}
    }

    return 1;
}

# fattura
sub f_Record() {
    my ($linea, @other) = @_;

    # @asar
    if ($record{'sottorecord'} =~ /^(\d{1})(0{1})(0{1}):(\d{4}):(0{4}):0(\s{10}):(0{2}):(\s{10}):(0{4})$/) {
        # non correttamente documentato ma arriva cosi'
    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

# inizio transazione
sub H_Record() {
    my ($linea, @other) = @_;
    
    # devo azzerare la variabile altrimenti crede di dover leggere dei "d" di scontrini precedenti
    $dettaglio_in_corso = 0;
    
    my $code1             ;
    my $code2             ;
    my $code3             ;
    my $codice_cassiere   ;
    my $info_aggiuntive   ;
    my $action_code       ;
    my $numero_fattura    ;
    my $ammontare         ;

    if ($record{'sottorecord'} =~ /^(\d{1})((0|5|6){1})((0|4){1}):(\d{4}):(.{16}):(\d{2})((\+|\d){6})((\+|\d){10})$/) {
        $code1           = $1;
        $code2           = $2;
        $code3           = $4;
        $codice_cassiere = $6;
        $info_aggiuntive = $7;
        $action_code     = $8;
        $numero_fattura  = $9;
        $ammontare       = $11;
    }
    else {
        return 0;
    }

    $tr_tipo = $code1;

    my $code123 = $code1.$code2.$code3;
    if($code123  !~ /(000|100|200|800|900)/) {
        print $log_file_handler "[$line_counter] Record H con code123 ($code123) mai incontrato\n";
    }

    if ($tr_aperta && ($tr_tipo != 9)) {
        print $log_file_handler "[$line_counter] WARNING! Nuovo Record H con transazione corrente ($tr_corrente) ancora aperta: $linea\n";
    }

	$cassiere         = $codice_cassiere;

    &Apertura_Transazione();

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    # se la transazione è di vendita
    if ($tr_tipo   eq 1) {
        # verifico che il record H di inizio scontrino abbia numero di riga 001
        if ($riga_transazione ne '001') {
            print $log_file_handler "[$line_counter] WARNING! Nuovo Record H con numero di riga diversa da 1 $linea\n";
        }
    }

    return 1;
}

# informazioni agguintive
sub i_Record() {
    my ($linea, @other) = @_;

    my $i_generica = qr/^((.){3})(.{41})$/;

    my $i_standard  = qr/^((\d){1})((0){1})((\d){1}):((\d){4}):((\d|\s){16}):((.){8})((.){2})((\d){1})((0|1|2|3){1})((.){6})$/;
    my $i_replay_e  = qr/^((.){1})((e){1})((.){1}):((.){3})((.){1}):((.){22}):((.){12})$/;
    my $i_replay_f  = qr/^((.){1})((f){1})((.){1}):((.){2})((.){2}):((.){10}):((.){1})((.){1})((.){22})$/;


    if ($record{'sottorecord'} =~ /$i_generica/) {
	my $i_type = $1;
	my $code1 = substr($i_type,0,1);
        my $code2 = substr($i_type,1,1);
        my $code3 = substr($i_type,2,1);

	    if ($record{'sottorecord'} =~ /$i_standard/) {
		$no_promo_flag = $17;
		if( $prev_record eq 'S') {
			$tipo_i = $i_type;
			$update_vendita = 1;
			&Update_Vendita();
		} else{
			print "Record i legato a record S ma che non segue un record S ($id_corrente): $linea\n";
		}
	    } elsif ($record{'sottorecord'} =~ /$i_replay_e/){
		# 1° record winepts
	    } elsif ($record{'sottorecord'} =~ /$i_replay_f/){
			# 2° record winepts
	    } else{
		return 0;
	    }
    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

# tessere fedeltà
sub k_Record() {
    my ($linea, @other) = @_;

    my $code1                ;
    my $code2                ;
    my $code3                ;
    my $categoria_tessera    ;
    my $codice_tessera       ;
    my $codice_tessera_padre ;
    my $nonusati             ;

    # ATTENZIONE!!!! ESISTONO DUE TIPI DI RECORD k
    if ($record{'sottorecord'} =~ /^(\d{1})(\d{1})((0|1|2|3|9){1}):(\d{4}):((\d|\s){16}):((\d|\s){16})(.{2})$/) {

        $code1               = $1;
        $code2               = $2;
        $code3               = $3;
        $categoria_tessera   = $5;
        $codice_tessera      = $6;
        $codice_tessera_padre= $8;
        $nonusati            = $9;
    }
    else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    # la tessera arriva in un formato a 10 cife: bisogna aggiungere lo 04 iniziale e il cin finale
    $codice_tessera =~ s/\s//ig;
    if($codice_tessera =~ /^[0-9]{13}$/) {
        # do nothing
    }
    elsif ($codice_tessera =~ /^[0-9]{10}$/){
        $codice_tessera ="04".$codice_tessera;
        if ( $codice_tessera =~ /^[0-9]{12}$/ ) {
            my $checksum;
            for (my $i = 1 ; $i < 12 ; $i += 2 ) {
                $checksum += ord( substr( $codice_tessera, $i, 1 ) ) - 48;
            }
            $checksum *= 3;
            for (my $i = 0 ; $i < 12 ; $i += 2 ) {
                $checksum += ord( substr( $codice_tessera, $i, 1 ) ) - 48;
            }
            my $cin = ( 10 - $checksum % 10 ) % 10;
            $codice_tessera = $codice_tessera.$cin;
        }
        else {
            return 0;
        }
    }
    else {
        return 0;
    }

    $tr_tessera = $codice_tessera;
    return 1;
}

# vendita
sub S_Record() {
    my ($linea, @other) = @_;
    my $code1               ;
    my $code2               ;
    my $code3               ;
    my $reparto             ;
    my $flag_farmaceutico   ;
    my $PLU     			;
    my $qta                 ;
    my $decimale            ;
    my $decimali_pxc        ;
    my $segno_qta           ;
    my $valore              ;
    my $valore_unit         ;

    #101:0001:   8007890713241+00010010*000000309
    if ($record{'sottorecord'} =~ /^(\d{1})(\d{1})(\d{1}):((\d|\s){4}):((A|\d|\s){16})((\+|\-|\d){5})((\d|\.){1})(\d{3})((\+|\-|\*){1})(\d{9})$/) {
        $code1              	= $1;
        $code2              	= $2;
        $code3              	= $3;
        $reparto            	= $4;
        $PLU                	= $6;
        $qta                	= $8;
        $decimale           	= $10;
        $decimali_pxc       	= $12;
        $segno_qta          	= $13;
        $valore_unit        	= $15;
    } else {
        return 0;
    }

	my $unita;
    # il contenuto del campo $decimale è '.' oppure 0
    if($decimale eq '.') {
        # quantità decimale
        $qta = $qta.$decimale.$decimali_pxc;
		if($qta <0){
			$unita = -1;
		}
		else{
			$unita = 1;
		}
    }
    else {
        # non faccio nulla
		$unita = $qta;
    }

    # se * allora il valore totale della vendita si ottiene moltiplicando il valore unitario per la qta
    if($segno_qta eq '*') {
        $valore = $valore_unit * $qta;
    }
    elsif ($segno_qta eq '-'){
        $valore = -1*$valore_unit;
    }
    else {
        $valore = $valore_unit;
    }


    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }


    # se la transazione  di vendita allora il valore partecipa al totale scontrino
    if (($tr_tipo eq 1) && ($code1 eq 1)) {
        # aggiorno la precedente vendita con il valore al netto di tutti gli sconti
        if($update_vendita > 0) {
            &Update_Vendita();
        }

        $quota_nonpagata	= 0;
        $valore_lordo   	= 0;
        $unita_vendute  	= 0;
				$qta_venduta    	= 0;
        $deprezzato     	= 0;
				$bollone        	= 0;
        $tipo_pagamento 	= '';
        $buono_sconto   	= 0;
        $pti_articolo   	= 0;
        $pti_target     	= 0;
        $tipo_i 		= '';
				$no_promo_flag		= 0;

        # inizio a costruire la vendita in corso
        $update_vendita = 1;
        $valore_lordo+=$valore;
        $qta_venduta+=$qta;
        $unita_vendute+=$unita;
				$totale_reparto{$reparto}+=$valore;
        $totale_scontrino_progressivo+=$valore;

        $PLU=~s/\s*//ig; #rimozione degli spazi
        my $barcode_itm = $PLU;
        my $articolo = &Transcode_Barcode($PLU,\$barcode_itm, $code1, $code2, $code3, $reparto, $valore);
				$articolo_corrente = $articolo;
        &AddCodiceArticolo($PLU, $articolo, $barcode_itm);
    }

    $id_ultima_vendita = $id_corrente;

    return 1;
}

# pagamento
sub T_Record() {
    my ($linea, @other) = @_;

    my $code1           ;
    my $code2           ;
    my $code3           ;
    my $cassiere        ;
    my $info_aggiuntive ;
    my $forma_pagamento ;
    my $contatore       ;
    my $ammontare       ;

    if ($record{'sottorecord'} =~ /^((0|1|2|3|4|8|9){1})((1|2|3|4|5){1})((\d){1}):(\d{4}):((.){16}):(\d{2})((\+|\-|\d){6})((\+|\-|\d){10})$/) {
        $code1              = $1;
        $code2              = $3;
        $code3              = $5;
        $cassiere           = $7;
        $info_aggiuntive    = $8;
        $forma_pagamento    = $10;
        $contatore          = $11;
        $ammontare          = $13;
    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    if ($tipo_pagamento !~ /$forma_pagamento/) {
        $tipo_pagamento.=$forma_pagamento.'|';
    }

    return 1;
}

# update coupon bruciati
sub u_Record() {
    my ($linea, @other) = @_;

    my $code1               ;
    my $code2               ;
    my $code3               ;
    my $cassiere            ;
    my $customerID          ;
    my $CMC_Counter         ;
    my $filename            ;

    if ($record{'sottorecord'} =~ /^((1){1})((0){1})((0){1}):(\d{4}):((\s|\d){16}):(\d{5})(.{8}\.\w{3}).$/) {

        $code1      = $1;
        $code2      = $3;
        $code3      = $5;
        $cassiere   = $7;
        $customerID = $8;
        $CMC_Counter= $10;
        $filename   = $11;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

# IVA
sub V_Record() {
    my ($linea, @other) = @_;

    my $code1           ;
    my $codice_iva      ;
    my $code3           ;
    my $cassiere        ;
    my $non_utilizzati  ;
    my $percentuale_iva ;
    my $action_code     ;
    my $contatore       ;
    my $ammontare_iva   ;

    if ($record{'sottorecord'} =~ /^((0|1|2|3|4|9){1})((0|1|2|3|4|5|6|7){1})((0|1|4){1}):(\d{4}):((.){10})((\d|\%|\.|\s){6}):(\d{2})((\+|\-|\d){6})((\+|\-|\d){10})$/) {

        $code1           = $1;
        $codice_iva      = $3;
        $code3           = $5;
        $cassiere        = $7;
        $non_utilizzati  = $8;
        $percentuale_iva = $10;
        $action_code     = $12;
        $contatore       = $13;
        $ammontare_iva   = $15;


    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

# code 128
sub X_Record() {
    my ($linea, @other) = @_;

    my $code1          ;
    my $code2          ;
    my $code3          ;
    my $codice_cassiere;
    my $ean128         ;

    if ($record{'sottorecord'} =~ /^((\d){1})((\d){1})((\d){1}):(\d{4}):((\d|\s){35})$/) {

        $code1          = $1;
        $code2          = $3;
        $code3          = $5;
        $codice_cassiere= $7;
        $ean128         = $8;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}


sub z_Record() {
    my ($linea, @other) = @_;

    my $code1          ;
    my $code2          ;
    my $code3          ;
    my $subdept        ;
    my $card_number    ;
    my $sell_type      ;
    my $transaction_id ;
    my $price_signe    ;
    my $price          ;


    if ($record{'sottorecord'} =~ /^((\d){1})((8|9){1})((0){1}):(\d{4}):((\d|\s){16})(\+|\-|)(0{8})(\+|\-|)(0{9})$/) {

        $code1          = $1;
        $code2          = $3;
        $code3          = $5;
        $subdept        = $7;
        $card_number    = $8;
        $sell_type      = $10;
        $transaction_id = $11;
        $price_signe    = $12;
        $price          = $13;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

sub Z_Record() {
    my ($linea, @other) = @_;

    my $code1          ;
    my $code2          ;
    my $code3          ;
    my $subdept        ;
    my $card_number    ;
    my $sell_type      ;
    my $transaction_id ;
    my $price_signe    ;
    my $price          ;

    if ($record{'sottorecord'} =~ /^((1){1})((0){1})((0){1}):(\d{4}):((\d|\s){16})(\+|\-|)(\d{8})(\+|\-|)(\d{9})$/) {

        $code1          = $1;
        $code2          = $3;
        $code3          = $5;
        $subdept        = $7;
        $card_number    = $8;
        $sell_type      = $10;
        $transaction_id = $11;
        $price_signe    = $12;
        $price          = $13;

    } else {
        return 0;
    }

    if (not &Check_Transazione($linea, $record{'num_transazione'})) {
    }

    return 1;
}

sub TestQuadraturaTransazione() {
	my($totale, @other)=@_;

	if ($tr_tipo eq 1) {
		# controllo il totale scontrino

		if (($totale_scontrino_progressivo-$totale)) {
			print ">>>>>>>>>>Non tornano i conti($totale_scontrino_progressivo <> $totale): $transazione_corrente<<<<<<<<<<<\n";
			#&Alert("Non tornano i conti($totale_scontrino_progressivo <> $totale): $transazione_corrente");
		}
		else{
			#print "OK! $transazione_corrente $totale_scontrino_progressivo = $totale): \n";
		}
	}
}

sub Alert() {
    my($msg,@other) = @_;

	$msg=~s/(\n|\r)/ /ig;

    # connessione al database
    #my $dbh = DBI->connect("DBI:mysql:$database:$hostname", $username, $password);
    if (! $dbh) {
        return;
    }

    my $sth = $dbh->prepare(qq{
            insert into etl.alert ( applicazione, parametri, tipo, messaggio, gestito)
						values ("datacollect_analyzer.pl", ?, "error", ?, 0);
		});

    #eseguo la query
    $sth->execute("", $msg);
}
