package listsample

import "time"

//PutBatchBuilder a builder to construct the PutBatch operation
type PutBatchBuilder struct {
	batch *PutBatch
}

//NewListDeltaBatchBuilder create a new empty builder
func NewListDeltaBatchBuilder() *PutBatchBuilder {
	return &PutBatchBuilder{
		batch: &PutBatch{
			deletes: []contactDeleteMutation{},
			updates: []contactWriteMutation{},
		},
	}
}

// AddUpdate adds an update operation to the batch
func (b *PutBatchBuilder) AddUpdate(userID, listID, contactID string, updatedAt time.Time) *PutBatchBuilder {
	b.batch.updates = append(b.batch.updates, contactWriteMutation{
		contactDeleteMutation: contactDeleteMutation{
			userID:    userID,
			listID:    listID,
			contactID: contactID,
		},
		updatedAt: updatedAt,
	})

	return b
}

// AddDelete adds a delete operation to the batch
func (b *PutBatchBuilder) AddDelete(userID, listID, contactID string) *PutBatchBuilder {
	b.batch.deletes = append(b.batch.deletes, contactDeleteMutation{
		userID:    userID,
		listID:    listID,
		contactID: contactID,
	})

	return b
}

// Build return the PutBatch
func (b *PutBatchBuilder) Build() *PutBatch {
	return b.batch
}
