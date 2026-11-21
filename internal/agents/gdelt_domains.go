package agents

// defaultGDELTDomains is the source-quality allowlist for GDELT. GDELT indexes
// tens of thousands of sites, many of them aggregators, content farms or
// reprints; the plan says to filter hard. Only domains with editorial
// accountability are kept. A domain matches its subdomains too, so
// "theguardian.com" also matches "www.theguardian.com".
//
// The list is intentionally broader than the RSS seed list: GDELT is where we
// see outlets that publish no usable public feed. Extend it with
// --gdelt-extra-domains for a deployment-specific addition.
var defaultGDELTDomains = []string{
	// International broadcasters and wire services.
	"bbc.co.uk", "bbc.com", "aljazeera.com", "dw.com", "france24.com",
	"npr.org", "cbc.ca", "abc.net.au", "theguardian.com", "reuters.com",
	"apnews.com", "afp.com", "cnn.com", "cnbc.com", "sky.com", "skynews.com",
	"euronews.com", "nhk.or.jp", "channelnewsasia.com", "cna.asia",
	"voanews.com", "rfi.fr", "trtworld.com", "aa.com.tr", "wionews.com",
	"france24.com", "rte.ie", "rnz.co.nz", "sbs.com.au", "9news.com.au",

	// Newspapers of record and national outlets.
	"nytimes.com", "washingtonpost.com", "wsj.com", "usatoday.com",
	"latimes.com", "politico.com", "politico.eu", "thehill.com", "axios.com",
	"theatlantic.com", "time.com", "foreignpolicy.com", "foreignaffairs.com",
	"propublica.org", "nbcnews.com", "cbsnews.com", "abcnews.go.com",
	"bloomberg.com", "ft.com", "economist.com", "telegraph.co.uk",
	"thetimes.co.uk", "independent.co.uk", "mirror.co.uk", "standard.co.uk",
	"irishtimes.com", "irishexaminer.com", "thejournal.ie",
	"lemonde.fr", "lefigaro.fr", "franceinfo.fr", "liberation.fr",
	"elpais.com", "elmundo.es", "eldiario.es", "lavanguardia.com",
	"corriere.it", "repubblica.it", "ansa.it", "lastampa.it",
	"spiegel.de", "zeit.de", "faz.net", "sueddeutsche.de", "welt.de",
	"nrc.nl", "volkskrant.nl", "dutchnews.nl",
	"lecho.be", "standaard.be", "vrt.be",
	"politiken.dk", "cphpost.dk", "thelocal.dk",
	"aftenposten.no", "nrk.no", "thelocal.no",
	"dn.se", "svd.se", "thelocal.se", "sverigesradio.se",
	"hs.fi", "yle.fi",
	"mbl.is", "ruv.is", "icelandmonitor.mbl.is",
	"independent.ie", "breakingnews.ie",
	"publico.pt", "dn.pt", "portugalresident.com",
	"ekathimerini.com", "kathimerini.gr",
	"cyprus-mail.com", "timesofmalta.com",
	"swissinfo.ch", "nzz.ch", "letemps.ch", "tagesanzeiger.ch",
	"derstandard.at", "diepresse.com", "orf.at",

	// Central and eastern Europe, Russia, the Balkans.
	"notesfrompoland.com", "wyborcza.pl", "onet.pl", "tvpworld.com",
	"expats.cz", "praguemorning.cz", "hungarytoday.hu", "telex.hu",
	"romania-insider.com", "hotnews.ro", "sofiaglobe.com", "novinite.com",
	"balkaninsight.com", "n1info.rs", "n1info.ba", "total-croatia-news.com",
	"english.sta.si", "b92.net", "danas.rs",
	"news.err.ee", "lrt.lt", "eng.lsm.lv",
	"kyivindependent.com", "kyivpost.com", "ukrinform.net", "pravda.com.ua",
	"themoscowtimes.com", "meduza.io", "novayagazeta.eu",
	"belsat.eu", "euroradio.fm",

	// Middle East and Africa.
	"arabnews.com", "gulfnews.com", "khaleejtimes.com", "thenationalnews.com",
	"alarabiya.net", "al-monitor.com", "middleeasteye.net",
	"middleeastmonitor.com", "timesofisrael.com", "jpost.com", "haaretz.com",
	"ynetnews.com", "maannews.net", "tehrantimes.com", "iranintl.com",
	"rudaw.net", "hurriyetdailynews.com", "dailysabah.com", "duvarenglish.com",
	"egyptindependent.com", "ahram.org.eg", "sudantribune.com",
	"moroccoworldnews.com", "theafricareport.com", "allafrica.com",
	"mg.co.za", "dailymaverick.co.za", "news24.com", "iol.co.za",
	"premiumtimesng.com", "punchng.com", "vanguardngr.com", "guardian.ng",
	"businessday.ng", "ghanaweb.com", "graphic.com.gh", "citinewsroom.com",
	"theeastafrican.co.ke", "nation.africa", "monitor.co.ug", "newvision.co.ug",
	"thecitizen.co.tz", "ethiopia-insight.com", "addisstandard.com",
	"newsday.co.zw", "lusakatimes.com", "thetimes.co.zm",
	"thenewhumanitarian.org", "reliefweb.int", "devex.com", "passblue.com",
	"globalvoices.org", "humanglemedia.com", "zawya.com",

	// South Asia.
	"thehindu.com", "timesofindia.indiatimes.com", "hindustantimes.com",
	"indianexpress.com", "ndtv.com", "thewire.in", "scroll.in", "theprint.in",
	"business-standard.com", "livemint.com", "economictimes.indiatimes.com",
	"telegraphindia.com", "deccanherald.com", "thestatesman.com",
	"firstpost.com", "thequint.com", "wionews.com",
	"dawn.com", "tribune.com.pk", "thenews.com.pk", "thedailystar.net",
	"dhakatribune.com", "kathmandupost.com", "myrepublica.nagariknetwork.com",
	"dailymirror.lk", "colombogazette.com", "tolonews.com", "pajhwok.com",

	// East and South-East Asia, Pacific.
	"scmp.com", "straitstimes.com", "todayonline.com", "channelnewsasia.com",
	"thejakartapost.com", "jakartaglobe.id", "antaranews.com",
	"bangkokpost.com", "nationthailand.com", "e.vnexpress.net",
	"vietnamnews.vn", "mb.com.ph", "newsinfo.inquirer.net", "rappler.com",
	"philstar.com", "japantimes.co.jp", "nhk.or.jp", "asahi.com",
	"mainichi.jp", "koreaherald.com", "koreatimes.co.kr", "yna.co.kr",
	"taipeitimes.com", "focustaiwan.tw", "asia.nikkei.com", "thediplomat.com",
	"eastasiaforum.org", "asiatimes.com", "nikkei.com",
	"smh.com.au", "theage.com.au", "news.com.au", "fijitimes.com.fj",
	"postcourier.com.pg", "nzherald.co.nz", "rnz.co.nz",

	// Americas.
	"theglobeandmail.com", "nationalpost.com", "ctvnews.ca", "globalnews.ca",
	"thestar.com", "mexiconewsdaily.com", "eluniversal.com.mx",
	"batimes.com.ar", "en.mercopress.com", "riotimesonline.com",
	"brazilian.report", "folha.uol.com.br", "estadao.com.br",
	"colombiareports.com", "santiagotimes.cl", "perureports.com",
	"jamaica-gleaner.com", "guardian.co.tt", "loopnews.com",
	"insightcrime.org", "dialogo-americas.com",

	// Climate, science, health, technology.
	"carbonbrief.org", "climatechangenews.com", "insideclimatenews.org",
	"grist.org", "e360.yale.edu", "mongabay.com", "nature.com", "science.org",
	"newscientist.com", "scientificamerican.com", "statnews.com",
	"technologyreview.com", "techcrunch.com", "theverge.com",
	"arstechnica.com", "wired.com", "theregister.com", "engadget.com",
	"venturebeat.com", "cnbc.com", "marketwatch.com", "finance.yahoo.com",
	"forbes.com", "businessinsider.com", "fortune.com",
	"defenseone.com", "breakingdefense.com", "warontherocks.com",
	"lawfaremedia.org", "justsecurity.org",

	// Multilateral and humanitarian.
	"un.org", "who.int", "unhcr.org", "icrc.org", "hrw.org", "amnesty.org",
	"worldbank.org", "imf.org", "nato.int", "europa.eu", "ec.europa.eu",
	"fao.org", "wfp.org", "unicef.org", "iom.int", "undp.org",
	"nrc.no", "rescue.org", "savechildren.org", "oxfam.org", "msf.org",
	"cdc.gov", "nih.gov", "ecdc.europa.eu", "ourworldindata.org",

	// Culture and sport, for the wire's breadth.
	"variety.com", "hollywoodreporter.com", "rollingstone.com", "billboard.com",
	"deadline.com", "artnet.com", "hyperallergic.com", "theartnewspaper.com",
	"espn.com", "bbc.com", "theathletic.com",
}
