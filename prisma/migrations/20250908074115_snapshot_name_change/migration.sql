/*
  Warnings:

  - You are about to drop the `Snapshot` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "public"."Snapshot";

-- CreateTable
CREATE TABLE "public"."snap" (
    "id" SERIAL NOT NULL,
    "user" TEXT NOT NULL,
    "open_orders" TEXT NOT NULL,
    "balance" INTEGER NOT NULL,
    "offsetId" INTEGER NOT NULL,

    CONSTRAINT "snap_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "snap_user_key" ON "public"."snap"("user");
