export const CollectionItemsListQuery = `
    query CollectionItemsListQuery($cursor: String, $sort: CollectionItemsSort!, $filter: CollectionItemsFilter, $collectionSlug: String!, $limit: Int) {
      collectionItems(
        collectionSlug: $collectionSlug
        cursor: $cursor
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
