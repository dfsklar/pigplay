$COUNThumans = 10;
$COUNTdays = 30;

$datestart = "20150101";

# Generate a probability table: what are the odds a given user will play on a given day?
# This sets up the *initial* probability... but after every day we nudge
# this a bit downwards -- we're basically simulating a funnel which is unidirectional,
# all players eventually losing interest...
@playOddsPerHuman;
for ($i = 0; $i < $COUNThumans; $i++) {    
    $playOddsPerHuman[$i] = rand(80);  # Odds go from 0 to 80 (representing percent)
}

for ($d = 0; $d < $COUNTdays; $d++) {
    for ($h = 0; $h < $COUNThumans; $h++) {
        # Did this human play on this day?
        $odds = (rand($playOddsPerHuman[$h]));
        #print STDERR $odds."\n";
        if (rand(100) < $playOddsPerHuman[$h]) {
            print ($datestart+$d);
            print ",USER" . (100+$h) . ",P\n";
        }
    }
    # Nudge this human's odds down just a tad, some random amount
    $playOddsPerHuman[$h] -= rand(4);
}
