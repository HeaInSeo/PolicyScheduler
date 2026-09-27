package storetest_test

import (
	"testing"

	"github.com/HeaInSeo/PolicyScheduler/admission"
	"github.com/HeaInSeo/PolicyScheduler/admission/storetest"
)

// MemoryStore covers every contract axis except durable reopen: it has no durable
// state, so Reopen stays nil and the C8 reopen axis reports NOT IMPLEMENTED (skip).
func TestMemoryStoreContract(t *testing.T) {
	storetest.Run(t, storetest.Harness{
		NewStore: func(*testing.T) admission.Store { return admission.NewMemoryStore() },
	})
}
