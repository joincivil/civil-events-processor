// Code generated by "stringer -type=GovernanceState"; DO NOT EDIT.

package model

import "strconv"

const _GovernanceState_name = "GovernanceStateNoneGovernanceStateAppliedGovernanceStateAppRemovedGovernanceStateChallengedGovernanceStateChallengeFailedGovernanceStateChallengeSucceededGovernanceStateFailedChallengeOverturnedGovernanceStateSuccessfulChallengeOverturnedGovernanceStateAppWhitelistedGovernanceStateRemovedGovernanceStateAppealGrantedGovernanceStateAppealRequestedGovernanceStateGrantedAppealChallengedGovernanceStateGrantedAppealConfirmedGovernanceStateGrantedAppealOverturnedGovernanceStateDepositGovernanceStateWithdrawalGovernanceStateRewardClaimedGovernanceStateTouchRemovedGovernanceStateListingWithdrawn"

var _GovernanceState_index = [...]uint16{0, 19, 41, 66, 91, 121, 154, 194, 238, 267, 289, 317, 347, 385, 422, 460, 482, 507, 535, 562, 593}

func (i GovernanceState) String() string {
	if i < 0 || i >= GovernanceState(len(_GovernanceState_index)-1) {
		return "GovernanceState(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _GovernanceState_name[_GovernanceState_index[i]:_GovernanceState_index[i+1]]
}
