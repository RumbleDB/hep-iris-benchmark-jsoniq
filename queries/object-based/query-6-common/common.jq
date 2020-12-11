module namespace o-6 = "common.jq";
import module namespace hep = "../../common/hep.jq";

declare function o-6:find-min-triplet($event) {
  (
    for $jet1 at $i in $event.jets[]
    for $jet2 at $j in $event.jets[]
    for $jet3 at $k in $event.jets[]
    where $i < $j and $j < $k
    order by abs(172.5 - hep:make-tri-jet($jet1, $jet2, $jet3).mass) ascending
    return [$jet1, $jet2, $jet3]
  )[1][]
};
