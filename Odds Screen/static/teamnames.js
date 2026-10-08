// Short team names for notes, chips and the tracker ("La Tech -2.5", "Chiefs ML").
// Pro leagues go by nickname (Kansas City Chiefs -> Chiefs); college by school
// with the nickname dropped (Louisiana Tech Bulldogs -> La Tech, Jacksonville
// State Gamecocks -> Jacksonville St). Shared by index.html and tracker.html.
(function () {
  // Nicknames longer than one word (Odds API spellings).
  const MULTI = ['Ragin Cajuns', "Ragin' Cajuns", 'Blue Devils', 'Crimson Tide', 'Golden Gophers', 'Red Raiders',
    'Fighting Illini', 'Fighting Irish', 'Golden Eagles', 'Golden Bears', 'Golden Hurricane', 'Golden Flashes',
    'Golden Knights', 'Golden Lions', 'Golden Griffins', 'Golden Rams', 'Golden Grizzlies', 'Yellow Jackets',
    'Blue Raiders', 'Blue Hens', 'Blue Jackets', 'Blue Demons', 'Blue Hose', 'Blue Jays', 'Maple Leafs', 'Red Wings',
    'Black Knights', 'Red Wolves', 'Mean Green', 'Sun Devils', 'Tar Heels', 'Nittany Lions', 'Horned Frogs',
    'Demon Deacons', 'Scarlet Knights', 'Thundering Herd', 'Green Wave', 'Rainbow Warriors', 'Wolf Pack',
    'Black Bears', 'Big Red', 'Big Green', 'Red Storm', 'Mountain Hawks', 'Fighting Hawks', 'Purple Eagles',
    'Great Danes', 'Red Foxes', 'River Hawks', 'Delta Devils', 'Running Rebels', 'Fighting Camels', 'Red Flash',
    'Purple Aces', 'Golden Panthers', 'Mocs', 'Trail Blazers', 'Red Sox', 'White Sox', 'Mountaineers',
    'Fighting Hawks', 'Lady Vols', 'Sea Wolves', 'Golden Seals', 'Runnin Utes', 'Runnin Bulldogs', 'Screaming Eagles'];
  const SCHOOL = {   // after the nickname is dropped
    'Louisiana': 'ULL', 'Louisiana Tech': 'La Tech', 'UL Monroe': 'ULM', 'Louisiana-Monroe': 'ULM',
    'Mississippi State': 'Miss St', 'Ole Miss': 'Ole Miss', 'Massachusetts': 'UMass', 'Connecticut': 'UConn',
    'North Carolina': 'UNC', 'North Carolina State': 'NC St', 'NC State': 'NC St', 'Florida International': 'FIU',
    'Florida Atlantic': 'FAU', 'Middle Tennessee': 'MTSU', 'Middle Tennessee State': 'MTSU',
    'Western Kentucky': 'WKU', 'Northern Illinois': 'NIU', 'Central Michigan': 'CMU', 'Eastern Michigan': 'EMU',
    'Western Michigan': 'WMU', 'Appalachian State': 'App St', 'Coastal Carolina': 'Coastal',
    'Georgia Southern': 'Ga Southern', 'Southern Mississippi': 'So Miss', 'Southern Miss': 'So Miss',
    'Texas-San Antonio': 'UTSA', 'UT San Antonio': 'UTSA', 'Texas-El Paso': 'UTEP', 'UTEP': 'UTEP',
    'Brigham Young': 'BYU', 'Southern Methodist': 'SMU', 'Texas Christian': 'TCU', 'Central Florida': 'UCF',
    'Miami (OH)': 'Miami OH', 'Miami (FL)': 'Miami', 'Kennesaw State': 'Kennesaw St', 'Sam Houston State': 'Sam Houston',
    'San José State': 'San Jose St', 'Hawaii': 'Hawaii', "Hawai'i": 'Hawaii', 'Virginia Tech': 'Va Tech',
    'Georgia Tech': 'Ga Tech', 'Texas Tech': 'Texas Tech', 'Boston College': 'BC', 'Bowling Green': 'BGSU',
    'East Carolina': 'ECU', 'Florida State': 'FSU', 'Pittsburgh': 'Pitt', 'Mississippi': 'Ole Miss',
  };
  const PRO = /^(americanfootball_nfl|basketball_nba|basketball_wnba|icehockey_nhl|baseball_mlb)$/;

  function nickname(name) {
    const m = MULTI.find(n => name.endsWith(' ' + n));
    return m || name.split(' ').pop();
  }
  window.shortTeam = function (name, sk) {
    if (!name) return '';
    name = String(name).trim();
    const nick = nickname(name);
    if (sk && PRO.test(sk)) return nick;
    const school = name.length > nick.length ? name.slice(0, name.length - nick.length).trim() : name;
    if (SCHOOL[school]) return SCHOOL[school];
    return school.replace(/\bState\b/g, 'St').replace(/\bUniversity\b/g, '').replace(/\s+/g, ' ').trim();
  };
})();
