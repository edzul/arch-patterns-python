import abc
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        # self.session.execute('INSERT INTO ??
        self.session.execute(
            'INSERT INTO batches(reference, sku, _purchased_quantity, eta)'
            ' VALUES(:batch_id, :sku, :qty, :eta)',
            dict(batch_id=batch.reference, sku=batch.sku,
                qty=batch._purchased_quantity, eta=batch.eta)
        )

        batch_row = self.session.execute(
            'SELECT id FROM batches where reference=:reference and sku=:sku ',
            dict(reference=batch.reference, sku=batch.sku)

        ).fetchone()

        for o in batch._allocations:
            self.session.execute(
                'INSERT INTO order_lines(orderid, sku, qty)'
                ' VALUES(:orderid, :sku, :qty)',
                dict(orderid=o.orderid, sku=o.sku, qty=o.qty)
            )
            
            orderline_row = self.session.execute(
                'SELECT id FROM order_lines WHERE '
                ' orderid=:orderid and sku=:sku',
                dict(orderid=o.orderid, sku=o.sku)
            ).fetchone()

            self.session.execute(
                'INSERT INTO allocations(orderline_id, batch_id)'
                ' VALUES(:orderline_id, :batch_id)',
                dict(orderline_id=orderline_row.id, batch_id=batch_row.id)
            )

    def get(self, reference) -> model.Batch:
        # self.session.execute('SELECT ??
        batch_row = self.session.execute(
            'SELECT id, reference, sku, _purchased_quantity, eta from batches where reference=:reference',
            dict(reference=reference)
        ).fetchone()

        if batch_row:
            batch = model.Batch(
                ref=batch_row.reference,
                sku=batch_row.sku,
                qty=batch_row._purchased_quantity,
                eta=batch_row.eta
            )

            allocations = list(self.session.execute(
                'SELECT orderid, sku, qty '
                ' FROM allocations '
                ' JOIN order_lines on allocations.orderline_id = order_lines.id'
                ' WHERE allocations.batch_id=:batch_id',
                dict(batch_id=batch_row.id))
            )

            batch._allocations = {model.OrderLine(o.orderid, o.sku, o.qty)
                                    for o in allocations}
        
        else:
            batch = None

        return batch
