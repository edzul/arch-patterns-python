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
