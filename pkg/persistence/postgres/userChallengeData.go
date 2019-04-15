package postgres

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/joincivil/civil-events-processor/pkg/model"

	"github.com/joincivil/go-common/pkg/numbers"
)

const (
	// UserChallengeDataTableName is the name of this table
	UserChallengeDataTableName = "user_challenge_data"
	defaultNilNum              = 0
)

// CreateUserChallengeDataQuery returns the query to return the userchallengedata table
func CreateUserChallengeDataQuery() string {
	return CreateUserChallengeDataQueryString(UserChallengeDataTableName)
}

// CreateUserChallengeDataQueryString returns the query to create this table
func CreateUserChallengeDataQueryString(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s(
            poll_id INT,
            poll_reveal_end_date INT,
            user_address TEXT,
            user_did_commit BOOL,
            user_did_reveal BOOL,
            did_user_collect BOOL,
            did_user_resuce BOOL,
            did_collect_amount NUMERIC,
            is_voter_winner BOOL,
            salt NUMERIC,
            choice NUMERIC,
            num_tokens NUMERIC,
            voter_reward NUMERIC,
            poll_type TEXT,
            parent_challenge_id NUMERIC,
            last_updated_timestamp INT
        );    
    `, tableName)
	return queryString
}

// UserChallengeDataTableIndices returns the query to create indices for this table
func UserChallengeDataTableIndices() string {
	return CreateUserChallengeDataTableIndicesString(UserChallengeDataTableName)
}

// CreateUserChallengeDataTableIndicesString returns the query to create indices for this table
func CreateUserChallengeDataTableIndicesString(tableName string) string {
	queryString := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS poll_id_idx ON %s (poll_id);
        CREATE INDEX IF NOT EXISTS user_address_idx ON %s (user_address);
    `, tableName, tableName)
	return queryString
}

// UserChallengeData is the postgres definition of model.UserChallengeData
type UserChallengeData struct {
	PollID            uint64
	PollRevealEndDate int64
	PollType          string
	UserAddress       string
	UserDidCommit     bool
	UserDidReveal     bool
	DidUserCollect    bool
	DidUserRescue     bool
	DidCollectAmount  float64
	IsVoterWinner     bool
	Salt              uint64
	Choice            uint64
	NumTokens         float64
	VoterReward       float64
	ParentChallengeID uint64
	LastUpdatedDateTs int64
}

// NewUserChallengeData creates a new UserChallengeData
func NewUserChallengeData(userChallengeData *model.UserChallengeData) *UserChallengeData {
	userChallengePgData := &UserChallengeData{}
	userChallengePgData.PollID = userChallengeData.PollID().Uint64()
	userChallengePgData.PollRevealEndDate = userChallengeData.PollRevealEndDate().Int64()
	userChallengePgData.PollType = userChallengeData.PollType()
	userChallengePgData.UserAddress = userChallengeData.UserAddress().Hex()
	userChallengePgData.UserDidCommit = userChallengeData.UserDidCommit()
	userChallengePgData.UserDidReveal = userChallengeData.UserDidReveal()
	userChallengePgData.DidUserCollect = userChallengeData.DidUserCollect()
	userChallengePgData.DidUserRescue = userChallengeData.DidUserRescue()
	userChallengePgData.IsVoterWinner = userChallengeData.IsVoterWinner()

	if userChallengeData.DidCollectAmount() != nil {
		userChallengePgData.DidCollectAmount = numbers.BigIntToFloat64(userChallengeData.DidCollectAmount())
	} else {
		userChallengePgData.DidCollectAmount = defaultNilNum
	}

	if userChallengeData.Salt() != nil {
		userChallengePgData.Salt = userChallengeData.Salt().Uint64()
	} else {
		userChallengePgData.Salt = defaultNilNum
	}

	if userChallengeData.Choice() != nil {
		userChallengePgData.Choice = userChallengeData.Choice().Uint64()
	} else {
		userChallengePgData.Choice = defaultNilNum
	}

	if userChallengeData.NumTokens() != nil {
		userChallengePgData.NumTokens = numbers.BigIntToFloat64(userChallengeData.NumTokens())
	} else {
		userChallengePgData.NumTokens = float64(defaultNilNum)
	}

	if userChallengeData.VoterReward() != nil {
		userChallengePgData.VoterReward = numbers.BigIntToFloat64(userChallengeData.VoterReward())
	} else {
		userChallengePgData.VoterReward = float64(defaultNilNum)
	}

	userChallengePgData.ParentChallengeID = userChallengeData.ParentChallengeID().Uint64()
	userChallengePgData.LastUpdatedDateTs = userChallengeData.LastUpdatedDateTs()
	return userChallengePgData
}

// DbToUserChallengeData creates a model.UserChallengeData from postgres.UserChallengeData
func (u *UserChallengeData) DbToUserChallengeData() *model.UserChallengeData {
	pollID := new(big.Int).SetUint64(u.PollID)
	pollRevealEndDate := new(big.Int).SetInt64(u.PollRevealEndDate)
	userAddress := common.HexToAddress(u.UserAddress)
	userDidCommit := u.UserDidCommit
	numTokens := numbers.Float64ToBigInt(u.NumTokens)
	userChallengeData := model.NewUserChallengeData(userAddress, pollID, numTokens,
		userDidCommit, pollRevealEndDate, u.PollType, u.LastUpdatedDateTs)
	userChallengeData.SetDidUserCollect(u.DidUserCollect)
	userChallengeData.SetUserDidReveal(u.UserDidReveal)
	userChallengeData.SetDidCollectAmount(numbers.Float64ToBigInt(u.DidCollectAmount))
	userChallengeData.SetIsVoterWinner(u.IsVoterWinner)
	userChallengeData.SetSalt(new(big.Int).SetUint64(u.Salt))
	userChallengeData.SetChoice(new(big.Int).SetUint64(u.Choice))
	userChallengeData.SetNumTokens(numbers.Float64ToBigInt(u.NumTokens))
	userChallengeData.SetVoterReward(numbers.Float64ToBigInt(u.VoterReward))
	userChallengeData.SetParentChallengeID(new(big.Int).SetUint64(u.ParentChallengeID))
	return userChallengeData
}
