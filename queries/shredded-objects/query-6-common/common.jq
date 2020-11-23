module namespace query-6 = "common.jq";
import module namespace hep = "../../common/hep.jq";

declare function query-6:find-min-triplet($event) {
  (
    for $jet1 at $i in $event.Jet[]
    for $jet2 at $j in $event.Jet[]
    for $jet3 at $k in $event.Jet[]
    where $i < $j and $j < $k
    order by abs(172.5 - hep:make-tri-jet($jet1, $jet2, $jet3).mass) ascending
    return [$jet1, $jet2, $jet3]
  )[1][]
};
