module namespace query-8 = "common.jq";
import module namespace hep = "../../common/hep.jq";

declare function query-8:find-closest-lepton-pair($leptons) {
  (
    for $lepton1 at $i in $leptons
    for $lepton2 at $j in $leptons
    where $i < $j
    where $lepton1.type = $lepton2.type and $lepton1.charge != $lepton2.charge
    order by abs(91.2 - hep:add-PtEtaPhiM($lepton1, $lepton2).mass) ascending
    return {"i": $i, "j": $j}
  )[1]
};
