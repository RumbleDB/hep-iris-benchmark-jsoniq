module namespace o-6 = "common.jq";
import module namespace hep = "../common/hep.jq";

declare function o-6:find-min-triplet($event) {
  (
    for $jet1 in $event.jets[]
    for $jet2 in $event.jets[]
    for $jet3 in $event.jets[]
    where $jet1.idx < $jet2.idx and $jet2.idx < $jet3.idx
    order by abs(172.5 - hep:TriJet($jet1, $jet2, $jet3).mass) ascending
    return [$jet1, $jet2, $jet3]
  )[1][]
};
