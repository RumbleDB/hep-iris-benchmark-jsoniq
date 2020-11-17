module namespace i-6 = "common.jq";
import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";

declare function i-6:find-min-triplet-idx($event) {
  (
    for $i in (1 to (size($event.Jet_pt) - 2))
    for $j in (($i + 1) to (size($event.Jet_pt) - 1))
    for $k in (($j + 1) to size($event.Jet_pt))
    let $particleOne := hep-i:MakeJetParticle($event, $i)
    let $particleTwo := hep-i:MakeJetParticle($event, $j)
    let $particleThree := hep-i:MakeJetParticle($event, $k)
    let $triJet := hep:TriJet($particleOne, $particleTwo, $particleThree)
    order by abs(172.5 - $triJet.mass) ascending
    return [$i, $j, $k]
  )[1]
};
