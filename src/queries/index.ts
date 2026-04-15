export const CollectionItemsListQuery = `
    query CollectionItemsListQuery($after: Cursor, $sort: CollectionItemsSort!, $filter: CollectionItemsFilter, $collectionSlug: String!, $limit: Int!) {
      collectionItems(
        collectionSlug: $collectionSlug
        after: $after
        sort: $sort
        filter: $filter
        limit: $limit
      ) {
        items {
          id
          tokenId
          name
          imageUrl
          bestListing {
            pricePerItem {
              token {
                unit
                symbol
              }
            }
          }
          lastSale {
            native {
              unit
            }
          }
          rarity {
            rank
            category
          }
        }
        nextPageCursor
      }
    }
  `;
