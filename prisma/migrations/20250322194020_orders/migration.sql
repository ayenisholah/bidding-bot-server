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

-- CreateTable
CREATE TABLE "Wallet" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "address" TEXT NOT NULL,
    "openseaApproval" BOOLEAN NOT NULL DEFAULT false,
    "magicedenApproval" BOOLEAN NOT NULL DEFAULT false
);

-- CreateIndex
CREATE INDEX "Order_taskId_idx" ON "Order"("taskId");

-- CreateIndex
CREATE INDEX "Order_marketplace_idx" ON "Order"("marketplace");

-- CreateIndex
CREATE INDEX "Order_taskId_marketplace_idx" ON "Order"("taskId", "marketplace");

-- CreateIndex
CREATE UNIQUE INDEX "Wallet_address_key" ON "Wallet"("address");

-- CreateIndex
CREATE INDEX "Wallet_address_idx" ON "Wallet"("address");
