-- CreateTable
CREATE TABLE "Order" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    "orderHash" TEXT NOT NULL,
    "expiresAt" DATETIME NOT NULL,
    "offerPrice" TEXT NOT NULL,
    "identifier" TEXT NOT NULL,
    "taskId" TEXT NOT NULL,
    "marketplace" TEXT NOT NULL,
    "slug" TEXT NOT NULL
);

-- CreateIndex
CREATE INDEX "Order_taskId_idx" ON "Order"("taskId");

-- CreateIndex
CREATE INDEX "Order_marketplace_idx" ON "Order"("marketplace");

-- CreateIndex
CREATE INDEX "Order_taskId_marketplace_idx" ON "Order"("taskId", "marketplace");
