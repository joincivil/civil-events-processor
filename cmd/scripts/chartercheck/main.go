package main

import (
	"flag"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/kelseyhightower/envconfig"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/joincivil/civil-events-processor/pkg/model"
	"github.com/joincivil/civil-events-processor/pkg/persistence"

	"github.com/joincivil/go-common/pkg/generated/contract"

	cbytes "github.com/joincivil/go-common/pkg/bytes"
	cconfig "github.com/joincivil/go-common/pkg/config"
)

// Config configures this script
type Config struct {
	EthAPIURL                string `envconfig:"eth_api_url" required:"true" desc:"Ethereum API address"`
	WetRun                   bool   `split_words:"true" desc:"If set to true, will perform mutations on the data"`
	PersisterPostgresAddress string `split_words:"true" desc:"If persister type is Postgresql, sets the address"`
	PersisterPostgresPort    int    `split_words:"true" desc:"If persister type is Postgresql, sets the port"`
	PersisterPostgresDbname  string `split_words:"true" desc:"If persister type is Postgresql, sets the database name"`
	PersisterPostgresUser    string `split_words:"true" desc:"If persister type is Postgresql, sets the database user"`
	PersisterPostgresPw      string `split_words:"true" desc:"If persister type is Postgresql, sets the database password"`
}

// PopulateFromEnv processes the environment vars, populates Config
func (c *Config) PopulateFromEnv() error {
	return envconfig.Process("script", c)
}

// OutputUsage prints the usage string to os.Stdout
func (c *Config) OutputUsage() {
	cconfig.OutputUsage(c, "script", "script")
}

// goes throughs all the charter revisions and dumps them out for debugging
// func checkRevisionFromContract(newsroomAddr common.Address, newsroom *contract.NewsroomContract,
// 	persister *persistence.PostgresPersister) error {
// 	revs, err := newsroom.RevisionCount(&bind.CallOpts{}, big.NewInt(0))
// 	if err != nil {
// 		fmt.Printf("err c: %v", err)
// 		return err
// 	}
// 	fmt.Printf("addr: %v\n", newsroomAddr.Hex())
// 	for i := 0; i < int(revs.Int64()); i++ {
// 		rev, err := newsroom.GetRevision(&bind.CallOpts{}, big.NewInt(0), big.NewInt(int64(i)))
// 		if err != nil {
// 			fmt.Printf("err d: %v", err)
// 			continue
// 		}
// 		fmt.Printf(
// 			"rev from contract:\nuri: %v\nts: %v\nhash: %v\nauthor: %v\n",
// 			rev.Uri,
// 			rev.Timestamp,
// 			cbytes.Byte32ToHexString(rev.ContentHash),
// 			rev.Author,
// 		)
// 	}
// 	return nil
// }

// ensures we are using the latest charter revision for a newsroom
func ensureListingLatestCharter(newsroomAddr common.Address, newsroom *contract.NewsroomContract,
	persister *persistence.PostgresPersister, wetRun bool) error {

	// Get latest revision
	revs, err := newsroom.RevisionCount(&bind.CallOpts{}, big.NewInt(0))
	if err != nil {
		fmt.Printf("err c: %v", err)
		return err
	}
	index := big.NewInt(revs.Int64() - 1)

	listing, err := persister.ListingByAddress(newsroomAddr)
	if err != nil {
		fmt.Printf("err z: %v", err)
		return err
	}

	if listing.Charter().ContentID().Int64() == int64(0) && listing.Charter().RevisionID().Int64() == index.Int64() {
		fmt.Printf("listing already has the latest revision: listing addr: %v", listing.ContractAddress().Hex())
		return nil
	}

	latestRev, err := newsroom.GetRevision(&bind.CallOpts{}, big.NewInt(0), index)
	if err != nil {
		fmt.Printf("err d: %v", err)
		return err
	}

	updatedFields := []string{"Charter"}
	updatedCharter := model.NewCharter(&model.CharterParams{
		URI:         latestRev.Uri,
		ContentID:   big.NewInt(0),
		RevisionID:  index,
		Signature:   latestRev.Signature,
		Author:      latestRev.Author,
		ContentHash: latestRev.ContentHash,
		Timestamp:   latestRev.Timestamp,
	})
	listing.SetCharter(updatedCharter)

	fmt.Printf(
		"updating listing charter: \nuri: %v\nhash: %v\nts: %v\ncontentid: %v\nrevid: %v\n",
		listing.Charter().URI(),
		cbytes.Byte32ToHexString(listing.Charter().ContentHash()),
		listing.Charter().Timestamp(),
		listing.Charter().ContentID(),
		listing.Charter().RevisionID(),
	)
	if wetRun {
		err = persister.UpdateListing(listing, updatedFields)
		if err != nil {
			fmt.Printf("err x: %v", err)
			return err
		}
	} else {
		fmt.Printf("WetRun = false, did not update in db\n")
	}
	return nil
}

// ensures we have all our charter revisions in the content_revision table
func ensureCharterContentRevisions(newsroomAddr common.Address, newsroom *contract.NewsroomContract,
	persister *persistence.PostgresPersister, wetRun bool) error {
	revs, err := newsroom.RevisionCount(&bind.CallOpts{}, big.NewInt(0))
	if err != nil {
		fmt.Printf("err c: %v", err)
		return err
	}

	crevs, err := persister.ContentRevisionsByCriteria(&model.ContentRevisionCriteria{
		ListingAddress: newsroomAddr.Hex(),
	})
	if err != nil {
		fmt.Printf("err e: %v", err)
		return err
	}

	revMap := map[int]*model.ContentRevision{}
	for ind, crev := range crevs {
		revMap[ind] = crev
	}

	for i := 0; i < int(revs.Int64()); i++ {
		_, ok := revMap[i]
		if !ok {
			rev, err := newsroom.GetRevision(&bind.CallOpts{}, big.NewInt(0), big.NewInt(int64(i)))
			if err != nil {
				fmt.Printf("err d: %v", err)
				continue
			}

			contentHash := cbytes.Byte32ToHexString(rev.ContentHash)
			revision := model.NewContentRevision(
				newsroomAddr,
				model.ArticlePayload{},
				contentHash,
				rev.Author,
				big.NewInt(0),
				big.NewInt(int64(i)),
				rev.Uri,
				rev.Timestamp.Int64(),
			)

			fmt.Printf(
				"add missing revision:\naddr: %v\nuri: %v\nts: %v\ncontentid: %v\nrevid: %v\n",
				newsroomAddr.Hex(),
				revision.RevisionURI(),
				revision.RevisionDateTs(),
				revision.ContractContentID(),
				revision.ContractRevisionID(),
			)
			if wetRun {
				err = persister.CreateContentRevision(revision)
				if err != nil {
					return err
				}
			} else {
				fmt.Printf("WetRun = false, did not update in db\n")
			}
		}
	}

	return nil
}

func main() {
	config := &Config{}
	flag.Usage = func() {
		config.OutputUsage()
		os.Exit(0)
	}
	flag.Parse()

	err := config.PopulateFromEnv()
	if err != nil {
		config.OutputUsage()
		os.Exit(2)
	}

	persister, err := persistence.NewPostgresPersister(
		config.PersisterPostgresAddress,
		config.PersisterPostgresPort,
		config.PersisterPostgresUser,
		config.PersisterPostgresPw,
		config.PersisterPostgresDbname,
	)
	if err != nil {
		fmt.Printf("err db: %v", err)
		return
	}

	client, err := ethclient.Dial(config.EthAPIURL)
	if err != nil {
		fmt.Printf("err a: %v", err)
		return
	}

	listings, err := persister.ListingsByCriteria(&model.ListingCriteria{
		Count: 100,
	})
	if err != nil {
		fmt.Printf("err listings: %v", err)
		return
	}

	for _, listing := range listings {
		newsroom, err := contract.NewNewsroomContract(listing.ContractAddress(), client)
		if err != nil {
			fmt.Printf("err b: %v", err)
			return
		}

		// err = checkRevisionFromContract(listing.ContractAddress(), newsroom, persister)
		// if err != nil {
		// 	fmt.Printf("err check content rev: %v", err)
		// 	return
		// }

		err = ensureCharterContentRevisions(listing.ContractAddress(), newsroom, persister, config.WetRun)
		if err != nil {
			fmt.Printf("err charter content rev: %v", err)
			return
		}

		err = ensureListingLatestCharter(listing.ContractAddress(), newsroom, persister, config.WetRun)
		if err != nil {
			fmt.Printf("err listing latest charter: %v", err)
			return
		}
		fmt.Printf("\n\n\n")
	}

	fmt.Println("done.")
}
