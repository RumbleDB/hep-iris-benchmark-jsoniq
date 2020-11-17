module namespace i-8 = "common.jq";
import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";

declare function i-8:find-closest-lepton-pair($leptons) {
  (
    for $i in (1 to (size($leptons.pt) - 1))
    for $j in (($i + 1) to size($leptons.pt))
    where $leptons.type[[$i]] = $leptons.type[[$j]] and
      $leptons.charge[[$i]] != $leptons.charge[[$j]]
    let $lepton1 := hep-i:MakeParticle($leptons, $i)
    let $lepton2 := hep-i:MakeParticle($leptons, $j)
    let $mass := hep:AddPtEtaPhiM2($lepton1, $lepton2).mass
    order by abs(91.2 - $mass) ascending
    return {"i": $i, "j": $j}
  )[1]
};
