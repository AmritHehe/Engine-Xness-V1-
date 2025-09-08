-- CreateTable
CREATE TABLE "public"."Snapshot" (
    "id" SERIAL NOT NULL,
    "user" TEXT NOT NULL,
    "open_orders" TEXT NOT NULL,
    "balance" INTEGER NOT NULL,
    "offset_Id" INTEGER NOT NULL,

    CONSTRAINT "Snapshot_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Snapshot_user_key" ON "public"."Snapshot"("user");
